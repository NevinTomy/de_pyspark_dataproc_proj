from src.main.utility.logging_config import logger
from google.cloud import storage
import traceback
# import os
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./svc_acct_key.json"

class GCSReader:
    def __init__(self):
        """ Initialize GCS client """
        try:
            self.gcs_client = storage.Client()
        except Exception as e:
            logger.error(f"Failed to initialize GCS client: {e}")
            raise

    def list_files(self, bucket, folder):
        """ List all files present in a GCS directory """
        try:
            gcs_bucket = self.gcs_client.get_bucket(bucket)
            blobs = gcs_bucket.list_blobs(prefix=folder)
            if blobs:
                all_files = [blob.name for blob in blobs if not blob.name.endswith('/')]
                logger.info(f"Files present in folder {folder} of bucket {bucket}: {all_files}")
                return all_files
            else:
                return []
        except Exception as e:
            err_msg = f"Error listing files: {e}"
            traceback_msg = traceback.format_exc()
            logger.error(f"Error message: {err_msg}\nTraceback message: {traceback_msg}")
            raise