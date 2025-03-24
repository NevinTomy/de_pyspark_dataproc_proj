from src.main.write.db_writer import DBWriter
from src.main.utility.logging_config import logger

from pyspark.sql.functions import *
from pyspark.sql.window import Window
import importlib
with open("/tmp/sales_data_etl/param/env.txt", "r") as file:
  env = file.read().strip()
config_module = f"param.{env}.config"
config = importlib.import_module(config_module)

def customer_mart_calc_table_write(final_customer_data_mart_df):
    try:
        cust_window = Window.partitionBy("customer_id", "sales_date_month")
        df_final_customer_data_mart = final_customer_data_mart_df \
                        .withColumn("sales_date_month", substring(col("sales_date"), 1, 7)) \
                        .withColumn("total_sales_per_month_per_customer", sum("total_cost").over(cust_window)) \
                        .select("customer_id", concat(col("first_name"), lit(" "), col("last_name")).alias("full_name"),
                                "address", "phone_number", "sales_date_month",
                                col("total_sales_per_month_per_customer").alias("total_sales")) \
                        .distinct()
        df_final_customer_data_mart.show()

        db_writer = DBWriter(url=config.url, properties=config.properties)
        db_writer.write_df_to_db(df_final_customer_data_mart, config.customer_data_mart_table)
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        raise