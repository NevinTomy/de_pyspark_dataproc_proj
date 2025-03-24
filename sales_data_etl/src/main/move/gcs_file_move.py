from src.main.utility.logging_config import logger

from google.cloud import storage
import traceback
# import os
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./svc_acct_key.json"

def move_gcs_files(bucket, source_file, dest_dir):
    """ Moves a file from one GCS directory to another """
    try:
        gcs_client = storage.Client()
        gcs_bucket = gcs_client.get_bucket(bucket)

        source_blob = gcs_bucket.blob(source_file)
        dest_blob_name = dest_dir + "" + source_file.split("/")[-1]
        gcs_bucket.copy_blob(source_blob, gcs_bucket, dest_blob_name)
        source_blob.delete()
        logger.info(f"File {source_file} moved to {dest_blob_name}")
    except Exception as e:
        err_msg = f"Error moving files: {str(e)}"
        traceback_msg = traceback.format_exc()
        logger.error(f"Error message: {err_msg}\nTraceback message: {traceback_msg}")
        raise