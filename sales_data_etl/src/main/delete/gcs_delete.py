from google.cloud import storage
import datetime

from src.main.utility.logging_config import logger

def gcs_delete_old_logs(bucket, gcs_log_dir, days_threshold):
    """ Delete log files from GCS directory that are older than supplied threshold days """
    try:
        gcs_client = storage.Client()
        gcs_bucket = gcs_client.bucket(bucket)
        blobs = gcs_bucket.list_blobs(prefix=gcs_log_dir)

        cutoff_date = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=int(days_threshold))

        deleted_blobs = []
        for blob in blobs:
            if blob.time_created and blob.time_created < cutoff_date:
                logger.info(f"Deleting log file {blob.name} from GCS directory gs://{bucket}/{gcs_log_dir}")
                blob.delete()
                deleted_blobs.append(blob.name)
        logger.info(f"Deleted {len(deleted_blobs)} log files that are older than {days_threshold} days from GCS log directory gs://{bucket}/{gcs_log_dir}")
    except Exception as e:
        logger.error(f"Error occurred while deleting log files: {str(e)}")
        raise
