/*-------------------------------------------------------------------------
 *
 * detach.c: send existing backup to S3 storage
 *
 * Portions Copyright (c) 2021, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */	

#include "pg_probackup.h"

#include "utils/thread.h"

static void *send_files_to_s3(void *arg);

/* Sanity checks for backup to s3 */
static void
init_s3(S3Params *s3_params)
{
	if (s3_params->s3_access_key_id == NULL)
		elog(ERROR, "required S3 parameter not specified: s3_access_key_id");
	if (s3_params->s3_secret_access_key == NULL)
		elog(ERROR, "required S3 parameter not specified: s3_secret_access_key");
	if (s3_params->s3_hostname == NULL)
		elog(ERROR, "required S3 parameter not specified: s3_hostname");
	if (s3_params->s3_bucket == NULL)
		elog(ERROR, "required S3 parameter not specified: s3_bucket");


    /*
     * This function must be called before any other libs3 function is called.
     * We call it once here. And set s3_deinitialize as atexit() callback.
     */
    s3_initialize(s3_params);
    pgut_atexit_push(s3_deinitialize, NULL);

    if (!s3_test_bucket(s3_params))
    elog(ERROR, "s3_bucket %s test is failed", s3_params->s3_bucket);
}

typedef struct send_to_s3_arg
{
	parray	   *files;
    char *base_path;
    S3Params *s3_params;
} send_to_s3_arg;


void
do_detach(time_t target_backup_id, S3Params *s3_params)
{
    pgBackup	*target_backup = NULL;
	parray 		*backup_list = NULL;
	pthread_t  *threads;
	send_to_s3_arg *threads_args;
    int i;

    if (instance_name == NULL)
		elog(ERROR, "required parameter not specified: --instance");

    init_s3(s3_params);

	backup_list = catalog_get_backup_list(instance_name, target_backup_id);

	if (parray_num(backup_list) != 1)
		elog(ERROR, "Failed to find backup %s", base36enc(target_backup_id));

	target_backup = (pgBackup *) parray_get(backup_list, 0);

    target_backup->files = get_backup_filelist(target_backup, true);

	parray_qsort(target_backup->files, pgFileCompareRelPathWithExternal);

	for (i = 0; i < parray_num(target_backup->files); i++)
	{
		pgFile	   *file = (pgFile *) parray_get(target_backup->files, i);
		pg_atomic_clear_flag(&file->lock);
    }

	threads = (pthread_t *) palloc(sizeof(pthread_t) * num_threads);
	threads_args = (send_to_s3_arg *) palloc(sizeof(send_to_s3_arg) *
												num_threads);

	thread_interrupted = false;

	/* Restore files into target directory */
	for (i = 0; i < num_threads; i++)
	{
		send_to_s3_arg *arg = &(threads_args[i]);

		arg->files = target_backup->files;
        arg->base_path = target_backup->database_dir;
        arg->s3_params = s3_params;

		elog(LOG, "Start thread %i", i + 1);

		pthread_create(&threads[i], NULL, send_files_to_s3, arg);
	}

	/* Wait theads */
	for (i = 0; i < num_threads; i++)
	{
		pthread_join(threads[i], NULL);
	}
}


static void *
send_files_to_s3(void *arg)
{
	send_to_s3_arg *arguments = (send_to_s3_arg *) arg;
	uint64      n_files;
    int i;
	char        file_fullpath[MAXPGPATH];

	n_files = (unsigned long) parray_num(arguments->files);

	for (i = 0; i < parray_num(arguments->files); i++)
	{
		pgFile	*file = (pgFile *) parray_get(arguments->files, i);

		if (!pg_atomic_test_set_flag(&file->lock))
			continue;

        if (!S_ISREG(file->mode))
                continue;
 
        if (!file->external_dir_num)
            join_path_components(file_fullpath, arguments->base_path, file->rel_path);

        // send file
        elog(INFO, "send file %s rel_path %s", file_fullpath, file->rel_path);
        s3_put_object(arguments->s3_params, file, file_fullpath);
    }

    return NULL;
}