from src.main.utility.logging_config import logger
from pyspark.sql.functions import *

def dimension_tables_join(final_df_to_process, customer_table_df, store_table_df, sales_team_table_df):
    logger.info("Joining final_df_to_process with customer_table_df")
    gcs_customer_df_join = final_df_to_process \
                        .join(customer_table_df, final_df_to_process["customer_id"] == customer_table_df["customer_id"], "inner") \
                        .drop("product_name", "price", "quantity", "additional_column", final_df_to_process["customer_id"], "customer_joining_date")

    logger.info("Joining gcs_customer_df_join with store_table_df")
    gcs_customer_store_df_join = gcs_customer_df_join \
                        .join(store_table_df, gcs_customer_df_join["store_id"] == store_table_df["id"], "inner") \
                        .drop("id", "store_pincode", "store_opening_date", "reviews")

    logger.info("Joining gcs_customer_store_df_join with sales_team_table_df")
    gcs_customer_store_sales_df_join =gcs_customer_store_df_join.alias("gcs_cs_df") \
                        .join(sales_team_table_df.alias("st"), col("st.id") == col("gcs_cs_df.sales_person_id"), "inner") \
                        .withColumn("sales_person_first_name", col("st.first_name")) \
                        .withColumn("sales_person_last_name", col("st.last_name")) \
                        .withColumn("sales_person_address", col("st.address")) \
                        .withColumn("sales_person_pincode", col("st.pincode")) \
                        .drop("id", sales_team_table_df["first_name"], sales_team_table_df["last_name"], sales_team_table_df["address"], sales_team_table_df["pincode"])

    return gcs_customer_store_sales_df_join