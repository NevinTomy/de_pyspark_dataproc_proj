from google.cloud import storage
import os

from src.main.utility.logging_config import logger

def upload_log_to_gcs(log_file_path, bucket, gcs_log_dir):
    """ Upload log file from local log directory to GCS log directory """
    try:
        gcs_client = storage.Client()
        gcs_bucket = gcs_client.bucket(bucket)
        log_filename = os.path.basename(log_file_path)
        blob = gcs_bucket.blob(f"{gcs_log_dir}/{log_filename}")

        blob.upload_from_filename(log_file_path)
        logger.info(f"Log file uploaded to gs://{bucket}/{gcs_log_dir}/{log_filename}")
    except Exception as e:
        logger.error(f"Error occurred while uploading log file: {str(e)}")
        raise
