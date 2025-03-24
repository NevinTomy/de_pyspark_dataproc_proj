from src.main.utility.logging_config import logger
import traceback

class FileWriter:
    def __init__(self, mode, file_format):
        self.mode = mode
        self.format = file_format

    def df_file_writer(self, df, gcs_dir, partition_by:list=None):
        try:
            if partition_by:
                partition_cols = str(partition_by)[1:-1]
                df.write.format(self.format) \
                    .option("header", "true") \
                    .mode(self.mode) \
                    .partitionBy(partition_cols) \
                    .option("path", gcs_dir) \
                    .save()
            else:
                df.write.format(self.format) \
                    .option("header", "true") \
                    .mode(self.mode) \
                    .option("path", gcs_dir) \
                    .save()
        except Exception as e:
            err_msg = f"Error writing data: {str(e)}"
            traceback_msg = traceback.format_exc()
            logger.error(f"Error message: {err_msg}\nTraceback message: {traceback_msg}")
            raise