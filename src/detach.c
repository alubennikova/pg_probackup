/*-------------------------------------------------------------------------
 *
 * detach.c: send existing backup to S3 storage
 *
 * Portions Copyright (c) 2021, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */	

#include "pg_probackup.h"

/* Sanity checks for backup to s3 */
static void
init_s3(S3Params s3_params)
{
	if (s3_params->s3_access_key_id == NULL)
		elog(ERROR, "required S3 parameter not specified: s3_access_key_id");
	if (s3_params->s3_secret_access_key == NULL)
		elog(ERROR, "required S3 parameter not specified: s3_secret_access_key");
	if (s3_params->s3_hostname == NULL)
		elog(ERROR, "required S3 parameter not specified: s3_hostname");
	if (s3_params->s3_bucketname == NULL)
		elog(ERROR, "required S3 parameter not specified: s3_bucket");


    /*
     * This function must be called before any other libs3 function is called.
     * We call it once here. And set s3_deinitialize as atexit() callback.
     */
    s3_initialize(s3_params);
    pgut_atexit_push(s3_deinitialize, NULL);

    if (!s3_test_bucket(s3_params))
    elog(ERROR, "s3_bucket %s test is failed", s3_params->s3_bucketname);
}


void
do_detach(time_t backup_id, S3Params s3_params)
{
    init_s3(s3_params);
}
