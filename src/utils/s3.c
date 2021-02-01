/*-------------------------------------------------------------------------
 *
 * s3.c: - utils to implement s3 storage
 *
 * Copyright (c) 2019-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <unistd.h>
#include <libs3.h>


static int statusG = 0;
static char errorDetailsG[4096] = { 0 };

static S3Protocol protocolG = S3ProtocolHTTPS;

/* Number of retries. TODO define it in S3Params ?*/
static int retriesG = 5;

static int should_retry(void);
static void printError(void);

static S3Status responsePropertiesCallback
				(const S3ResponseProperties *properties, void *callbackData);
static void responseCompleteCallback(S3Status status,
									const S3ErrorDetails *error,
									void *callbackData);
static int putObjectDataCallback(int bufferSize, char *buffer,
                                 void *callbackData);
static S3Status getObjectDataCallback(int bufferSize, const char *buffer,
                                      void *callbackData);

/* ------ Wrappers on low-level s3 funcitons ----- */

void
s3_initialize(S3Params s3_params)
{
	S3Status status;

	if ((status = S3_initialize("s3", S3_INIT_ALL, s3_params->s3_hostname))
        != S3StatusOK)
		elog(ERROR, "Failed to initialize libs3: %s",
			 S3_get_status_name(status));
}

/* For use in atexit callback */
void
s3_deinitialize(bool fatal, void *userdata)
{
	S3_deinitialize();
}

// create bucket
int
s3_test_bucket(S3Params s3_params)
{
	S3ResponseHandler responseHandler =
	{
		&responsePropertiesCallback, &responseCompleteCallback
	};

	char 		locationConstraint[64];
	int 		result;

	elog(INFO, "s3_test_bucket: begin:\n s3_access_key_id %s\n"
			   "s3_secret_access_key %s\ns3_hostname %s\n bucket %s",
				s3_params->s3_access_key_id, s3_params->s3_secret_access_key,
				s3_params->s3_hostname, s3_params->s3_bucket);

	do {
		S3_test_bucket(protocolG,
					   s3_params->s3_force_path_style?S3UriStylePath:S3UriStyleVirtualHost,
					   s3_params->s3_access_key_id,
					   s3_params->s3_secret_access_key,
					   NULL, s3_params->s3_bucket,
					   sizeof(locationConstraint),
					   locationConstraint, NULL, &responseHandler, 0);

		elog(INFO, "s3_test_bucket: %s. status %d", s3_params->s3_bucket, statusG);
	} while (S3_status_is_retryable(statusG) && should_retry());

	result = statusG == S3StatusOK ? 1 : 0;

	if (!result)
		printError();
	else
		elog(INFO, "s3_test_bucket succeed %s", s3_params->s3_bucket);

	return result;
}

// put object and helper

typedef struct put_object_callback_data
{
    FILE *infile;
    uint64_t contentLength;
	uint64_t originalContentLength;
	pgFile *file;
} put_object_callback_data;


void
s3_put_object(S3Params S3Params, pgFile *file)
{
	S3NameValue metaProperties[S3_MAX_METADATA_COUNT];
	put_object_callback_data data;
	char path[MAXPGPATH] = "/tmp/file";

	S3BucketContext bucketContext =
	{
		0,
		s3_params->s3_bucket,
		protocolG,
		s3_params->s3_force_path_style?S3UriStylePath:S3UriStyleVirtualHost,
		s3_params->s3_access_key_id,
		s3_params->s3_secret_access_key,
	};

	S3PutProperties putProperties =
	{
		0,
		0,
		0,
		0,
		0,
		-1,
		S3CannedAclPrivate,
		0,
		metaProperties,
	};

	S3PutObjectHandler putObjectHandler =
	{
		{ &responsePropertiesCallback, &responseCompleteCallback },
		&putObjectDataCallback
	};

	data.infile = 0;
	data.file = file;

	if (file->name)
	{
		if (!file->size)
		{
			struct stat statbuf;
			/* Stat the file to get its length */
			if (stat(path, &statbuf) == -1) 
			{
				elog(ERROR, "ERROR: Failed to stat file %s: ",
						path);
			}
			file->size = statbuf.st_size;
		}
		/* Open the file */
		if (!(data.infile = fopen(path, "r")))
		{
			elog(ERROR, "ERROR: Failed to open input file %s: ",
						path);
		}
	}

	data.contentLength = file->size;
	data.originalContentLength = file->size;

	INIT_FILE_CRC32(true, data.file->crc);

	do
	{
		char s3_filepath[MAXPGPATH];

		//TODO create a function (or macro) to generate the path for s3
		snprintf(s3_filepath, MAXPGPATH, "%s/%s", base36enc(current.start_time),
						file->rel_path);

		S3_put_object(&bucketContext, s3_filepath, file->size,
					  &putProperties, 0, &putObjectHandler, &data);

	} while (S3_status_is_retryable(statusG) && should_retry());

	if (data.infile)
		fclose(data.infile);

	FIN_FILE_CRC32(true, data.file->crc);
	elog(INFO, "done put_object crc %X file %s ",
			data.file->crc, file->name);

	if (statusG != S3StatusOK)
	{
		printError();
	}
	else if (data.contentLength)
		elog(ERROR, "ERROR: Failed to read remaining %llu bytes from "
				"input", (unsigned long long) data.contentLength);
	//file->write_size = file->size;
}

// get object

// bool
// s3_get_object(S3Params S3Params, pgFile *file)
// {
//     FILE *outfile = 0;

// 		S3BucketContext bucketContext =
// 	{
// 		0,
// 		s3_params->s3_bucket,
// 		protocolG,
// 		s3_params->s3_force_path_style?S3UriStylePath:S3UriStyleVirtualHost,
// 		s3_params->s3_access_key_id,
// 		s3_params->s3_secret_access_key,
// 	};


//     S3GetConditions getConditions =
//     {
//         -1,
//         -1,
//         0,
//         0
//     };

//     S3GetObjectHandler getObjectHandler =
//     {
//         { &responsePropertiesCallback, &responseCompleteCallback },
//         &getObjectDataCallback
//     };

	
//     if (file->path && S_ISREG(file->mode) && !file->is_datafile)
// 	{
//         // Stat the file, and if it doesn't exist, open it in w mode
//         struct stat buf;

// 		if (stat(file->path, &buf) == -1)
// 		{
// 		   elog(INFO, "s3_get_object. open outfile %s", file->path);
//            outfile = fopen(file->path, "w");
//         }
//         else
// 			elog(ERROR, "Failed to create file %s", file->path);

//         if (!outfile)
//             elog(ERROR, "\nERROR: Failed to open output file %s: ",
//                     file->path);
//     }

//     do {
// 		char tmp_filepath[MAXPGPATH];
// 		char s3_filepath[MAXPGPATH];

// 		//TODO create a function (or macro) to generate the path for s3
// 		snprintf(tmp_filepath, MAXPGPATH, "%s", GetRelativePath(file->path, backup_instance_path));
// 		snprintf(s3_filepath, MAXPGPATH, "%s/%s", base36enc(current.start_time),
// 						 tmp_filepath+strlen("XXXXXX/database/"));

// 		elog(INFO, "do get_object file %s from %s/%s backup_path %s",
// 			file->name, s3_params->s3_bucket, s3_filepath, backup_instance_path);

// 		S3_get_object(&bucketContext, s3_filepath, &getConditions, 0,
//                       0,0, &getObjectHandler, outfile);
//     } while (S3_status_is_retryable(statusG) && should_retry());

//     if (statusG != S3StatusOK) {
//         printError();
// 		return false;
//     }

//     fclose(outfile);
// 	return true;
// }


/* ------ Internal helper functions ----- */

static int should_retry(void)
{
    if (retriesG--)
	{
        /* Sleep before next retry; start out with a 1 second sleep */
        static int retrySleepInterval = 1;
        sleep(retrySleepInterval);
        /* Next sleep 1 second longer */
        retrySleepInterval++;
        return 1;
    }

    return 0;
}

static void printError(void)
{
	if (statusG < S3StatusErrorAccessDenied)
		elog(INFO, "S3 error: %s\n", S3_get_status_name(statusG));
	else
		elog(INFO, "S3 error: %s. %s",
			 S3_get_status_name(statusG), errorDetailsG);
}


/* ------ Internal callback functions ----- */

// response properties callback ----------------------------------------------

// This callback does the same thing for every request type: prints out the
// properties if the user has requested them to be so
static S3Status responsePropertiesCallback
    (const S3ResponseProperties *properties, void *callbackData)
{
    return S3StatusOK;
}

// This callback does the same thing for every request type: saves the status
// and error stuff in global variables
static void responseCompleteCallback(S3Status status,
									const S3ErrorDetails *error,
									void *callbackData)
{
	int len = 0;

	(void) callbackData;

	statusG = status;
	// Compose the error details message now, although we might not use it.
	// Can't just save a pointer to [error] since it's not guaranteed to last
	// beyond this callback
	if (error && error->message) {
		len += snprintf(&(errorDetailsG[len]), sizeof(errorDetailsG) - len,
						"  Message: %s\n", error->message);
	}
	if (error && error->resource) {
		len += snprintf(&(errorDetailsG[len]), sizeof(errorDetailsG) - len,
						"  Resource: %s\n", error->resource);
	}
	if (error && error->furtherDetails) {
		len += snprintf(&(errorDetailsG[len]), sizeof(errorDetailsG) - len,
						"  Further Details: %s\n", error->furtherDetails);
	}
	if (error && error->extraDetailsCount)
	{
		int i;

		len += snprintf(&(errorDetailsG[len]), sizeof(errorDetailsG) - len,
						"%s", "  Extra Details:\n");
		for (i = 0; i < error->extraDetailsCount; i++) {
			len += snprintf(&(errorDetailsG[len]),
							sizeof(errorDetailsG) - len, "    %s: %s\n",
							error->extraDetails[i].name,
							error->extraDetails[i].value);
		}
	}
}

static int putObjectDataCallback(int bufferSize, char *buffer,
                                 void *callbackData)
{
    put_object_callback_data *data =
        (put_object_callback_data *) callbackData;

    int ret = 0;

    if (data->contentLength) {
        int toRead = ((data->contentLength > (unsigned) bufferSize) ?
                      (unsigned) bufferSize : data->contentLength);
		if (data->infile) {
            ret = fread(buffer, 1, toRead, data->infile);
        }
    }

   COMP_FILE_CRC32(true, data->file->crc, buffer, ret);

    //elog(INFO, "putObjectDataCallback. contentLength %lu ret %d", data->contentLength, ret);
    data->contentLength -= ret;

	data->file->write_size+=ret;
	

    return ret;
}

static S3Status getObjectDataCallback(int bufferSize, const char *buffer,
                                      void *callbackData)
{
    FILE *outfile = (FILE *) callbackData;

    size_t wrote = fwrite(buffer, 1, bufferSize, outfile);

    return ((wrote < (size_t) bufferSize) ?
            S3StatusAbortedByCallback : S3StatusOK);
}


