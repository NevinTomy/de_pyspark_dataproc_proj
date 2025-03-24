from src.main.utility.logging_config import logger

class DBWriter:
    def __init__(self, url, properties):
        self.url = url
        self.properties = properties

    def write_df_to_db(self, df, table_name):
        try:
            df.write.jdbc(url=self.url,
                          table=table_name,
                          mode="append",
                          properties=self.properties)
            logger.info(f"Data from DF {df} successfully written to table {table_name}")
        except Exception as e:
            logger.error(f"Error occurred: {str(e)}")
            raise