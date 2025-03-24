from src.main.write.db_writer import DBWriter
from src.main.utility.logging_config import logger

from pyspark.sql.functions import *
from pyspark.sql.window import Window
import importlib
with open("/tmp/sales_data_etl/param/env.txt", "r") as file:
  env = file.read().strip()
config_module = f"param.{env}.config"
config = importlib.import_module(config_module)

def sales_mart_calc_table_write(final_sales_data_mart_df):
    try:
        sales_window = Window.partitionBy("store_id", "sales_person_id", "sales_month")
        df_final_sales_data_mart = final_sales_data_mart_df \
                        .withColumn("total_sales_every_month", sum("total_cost").over(sales_window)) \
                        .select("store_id", "sales_person_id",
                                concat("sales_person_first_name", lit(" "), "sales_person_last_name").alias("full_name"),
                                "sales_month", col("total_sales_every_month").alias("total_sales")) \
                        .distinct()

        sales_rank_window = Window.partitionBy("store_id", "sales_month").orderBy(col("total_sales").desc())
        df_final_sales_data_mart_table = df_final_sales_data_mart \
                        .withColumn("rnk", rank().over(sales_rank_window)) \
                        .withColumn("incentive", when(col("rnk") == 1, col("total_sales")*0.01).otherwise(lit(0))) \
                        .withColumn("incentive", round(col("incentive"), 2)) \
                        .select("store_id", "sales_person_id", "full_name", "sales_month", "total_sales", "incentive")
        df_final_sales_data_mart_table.show()

        db_writer = DBWriter(url=config.url, properties=config.properties)
        db_writer.write_df_to_db(df_final_sales_data_mart_table, config.sales_team_data_mart_table)
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        raise