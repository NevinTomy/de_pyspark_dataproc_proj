from src.main.utility.logging_config import logger
from src.main.delete.gcs_delete import gcs_delete_old_logs
from src.main.read.database_read import DBReader
from src.main.transformations.customer_mart_sql_transform_write import customer_mart_calc_table_write
from src.main.transformations.dimension_tables_join import dimension_tables_join
from src.main.transformations.sales_mart_sql_transform_write import sales_mart_calc_table_write
from src.main.upload.gcs_upload import upload_log_to_gcs
from src.main.utility.spark_session import *
from src.main.utility.my_sql_session import *
from src.main.read.gcs_read import *
from src.main.move.gcs_file_move import *
from src.main.write.file_writer import FileWriter

with open("/tmp/sales_data_etl/param/env.txt", "r") as file:
  env = file.read().strip()
logger.info(f"Environment: {env}")
config_module = f"param.{env}.config"
config = importlib.import_module(config_module)

gcs_reader = GCSReader()
source_files = gcs_reader.list_files(config.bucket_name, config.gcs_source_directory)

csv_files = []
error_files=[]
if source_files:
  for file in source_files:
    if file.endswith(".csv"):
      csv_files.append(file)
    else:
      error_files.append(file)

  if csv_files:
    file_name = [file.split('/')[-1] for file in csv_files]
    file_check_query = f"""
      SELECT DISTINCT file_name FROM
      {config.database_name}.{config.product_staging_table}
      WHERE file_name IN ({str(file_name)[1:-1]}) AND status = 'A'
    """
    logger.info(f"File check query: {file_check_query}")

    conn = get_mysql_conn()
    cursor = conn.cursor()
    cursor.execute(file_check_query)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    if data:
      logger.info("Last run was a failure. Please check!")
    else:
      logger.info("No match found. Last run was successful!")
  else:
    logger.error("No csv data available to process the request.")
    raise Exception("No csv data available to process the request.")

else:
  logger.error("No file present in GCS source directory for processing.")
  raise Exception("There is no data to process.")

logger.info("*************************** List of CSV files ***************************")
logger.info(f"List of CSV files that needs to be processed: {csv_files}")
logger.info("*************************** Creating Spark session ***************************")

spark = spark_session()

logger.info("*************************** Spark session created ***************************")

correct_files = []
for file in csv_files:
  data_schema = spark.read.format("csv") \
                .option("header", "true") \
                .load(file).columns
  logger.info(f"Schema for {file} is {data_schema}")
  logger.info(f"Mandatory columns schema is {config.mandatory_columns}")
  missing_columns = set(config.mandatory_columns) - set(data_schema)
  logger.info(f"Missing columns are {missing_columns}")

  if missing_columns:
    error_files.append(file)
  else:
    logger.info(f"No missing columns for {file}")
    correct_files.append(file)

logger.info(f"List of correct files: {correct_files}")
logger.info(f"List of error file: {error_files}")

if error_files:
  logger.info("******************** Moving error files to error directory ********************")
  for err_file in error_files:
    move_gcs_files(config.bucket_name, err_file, config.gcs_error_directory)
else:
  logger.info("There is no error file in the GCS directory.")

insert_statements = []
current_datetime = datetime.datetime.now()
formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

if correct_files:
  logger.info("********************* Updating product_staging_table that we have started our process *********************")
  for corr_file in correct_files:
    insert_query = f"""
              INSERT INTO {config.database_name}.{config.product_staging_table} (file_name, file_location, created_date, status)
              VALUES ({corr_file.split('/')[-1]}, gs://{config.bucket_name}/{corr_file}, {formatted_datetime}, 'A')
          """
    insert_statements.append(insert_query)
  logger.info(f"Insert statements created for staging table --> {insert_statements}")
  logger.info("********************* Connecting to MySQL server *********************")
  conn = get_mysql_conn()
  cursor = conn.cursor()
  logger.info("********************* MySQL server connected successfully *********************")
  for statement in insert_statements:
    cursor.execute(statement)
    conn.commit()
  cursor.close()
  conn.close()
else:
  logger.info("********************* There is no file to process *********************")
  raise Exception("********************* No data available with correct format to process *********************")

logger.info("************************* Staging table updated successfully! ************************")
logger.info("************************* Fixing extra columns from source, if any ***********************")

schema = StructType([
  StructField("customer_id", IntegerType(), True),
  StructField("store_id", IntegerType(), True),
  StructField("product_name", StringType(), True),
  StructField("sales_date", DateType(), True),
  StructField("sales_person_id", IntegerType(), True),
  StructField("price", FloatType(), True),
  StructField("quantity", IntegerType(), True),
  StructField("total_cost", FloatType(), True),
  StructField("additional_column", StringType(), True)
])

final_df_to_process = spark.createDataFrame([], schema=schema)

for data in correct_files:
  data_df = spark.read.format("csv") \
          .option("header", "true") \
          .option("inferSchema", "true") \
          .load(data)
  data_schema = data_df.columns
  extra_columns = list(set(data_schema) - set(config.mandatory_columns))
  logger.info(f"Extra columns present at source is {extra_columns}")

  if extra_columns:
    data_df = data_df.withColumn("additional_column", concat_ws(", ", *extra_columns)) \
          .select("customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost","additional_column")
    logger.info(f"Processed {data} and added 'additional column'")
  else:
    data_df = data_df.withColumn("additional_column", lit(None)) \
          .select("customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost","additional_column")

  final_df_to_process = final_df_to_process.union(data_df)

logger.info("*********************** Final dataframe from source is created for processing ***********************")
final_df_to_process.show()

logger.info("Initialize DB reader ...")
db_client = DBReader(config.url, config.properties)

logger.info("*************************** Loading customer table into customer_table_df **************************")
customer_table_df = db_client.create_df(spark=spark, table_name=config.customer_table_name)

logger.info("*************************** Loading product table into customer_table_df **************************")
product_table_df = db_client.create_df(spark=spark, table_name=config.product_table)

logger.info("*************************** Loading sales team table into customer_table_df **************************")
sales_team_table_df = db_client.create_df(spark=spark, table_name=config.sales_team_table)

logger.info("*************************** Loading store table into customer_table_df **************************")
store_table_df = db_client.create_df(spark=spark, table_name=config.store_table)

logger.info("*************************** Loading product staging table into customer_table_df **************************")
product_staging_table_df = db_client.create_df(spark=spark, table_name=config.product_staging_table)

gcs_customer_store_sales_df_join = dimension_tables_join(final_df_to_process, customer_table_df, store_table_df, sales_team_table_df)

logger.info("*************************** Final enriched data ***************************")
gcs_customer_store_sales_df_join.show()

logger.info("*************************** Write data into customer data mart ***************************")
final_customer_data_mart_df = gcs_customer_store_sales_df_join \
                  .select("customer_id", "first_name", "last_name", "address", "pincode",
                          "phone_number", "sales_date", "total_cost")

logger.info("*************************** Final data for customer data mart ***************************")
final_customer_data_mart_df.show()

df_file_writer = FileWriter("overwrite", "parquet")
current_epoch = int(datetime.datetime.now().timestamp()) * 1000
gcs_customer_datamart_dir = f"gs://{config.bucket_name}/{config.gcs_customer_datamart_directory}/{current_epoch}"
df_file_writer.df_file_writer(final_customer_data_mart_df, gcs_customer_datamart_dir)
logger.info(f"*************************** Customer data written to GCS directory {gcs_customer_datamart_dir} ***************************")

logger.info("*************************** Write data into sales data mart ***************************")
final_sales_data_mart_df = gcs_customer_store_sales_df_join \
                  .select("store_id", "sales_person_id", "sales_person_first_name", "sales_person_last_name", "store_manager_name",
                          "manager_id", "is_manager", "sales_person_address", "sales_person_pincode", "sales_date", "total_cost",
                          expr("SUBSTRING(sales_date, 1, 7) AS sales_month"))

logger.info("*************************** Final data for sales data mart ***************************")
final_sales_data_mart_df.show()

current_epoch = int(datetime.datetime.now().timestamp()) * 1000
gcs_sales_datamart_dir = f"gs://{config.bucket_name}/{config.gcs_sales_datamart_directory}/{current_epoch}"
df_file_writer.df_file_writer(final_sales_data_mart_df, gcs_sales_datamart_dir)
logger.info(f"*************************** Sales data written to GCS directory {gcs_sales_datamart_dir} ***************************")

current_epoch = int(datetime.datetime.now().timestamp()) * 1000
gcs_sales_partitioned_datamart_dir = f"gs://{config.bucket_name}/{config.gcs_sales_partitioned_datamart_directory}/{current_epoch}"
df_file_writer.df_file_writer(final_sales_data_mart_df, gcs_sales_partitioned_datamart_dir, ["sales_month", "store_id"])
logger.info(f"*************************** Sales partitioned data written to GCS directory {gcs_sales_partitioned_datamart_dir} ***************************")

logger.info("**************************** Calculating purchased amount per month per customer ******************************")
customer_mart_calc_table_write(final_customer_data_mart_df)
logger.info("**************************** Calculated purchased amount per month per customer and written to table ******************************")

logger.info("**************************** Calculating sales incentive per store per month ******************************")
sales_mart_calc_table_write(final_sales_data_mart_df)
logger.info("**************************** Calculated sales incentive per store per month and written to table ******************************")

update_statements = []
current_datetime = datetime.datetime.now()
formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

if correct_files:
  logger.info("******************** Moving processed CSV files to processed directory ********************")
  for corr_file in correct_files:
    move_gcs_files(config.bucket_name, corr_file, config.gcs_processed_directory)
    filename = os.path.basename(corr_file)
    update_statement = f"""
          UPDATE {config.database_name}.{config.product_staging_table}
          SET status = 'I',
              updated_date = {formatted_datetime}
          WHERE file_name = {filename}
      """
    update_statements.append(update_statement)

  logger.info(f"Update statements created for staging table --> {update_statements}")
  logging.info("*********************** Connecting to MySQL server ***********************")
  conn = get_mysql_conn()
  cursor = conn.cursor()
  logging.info("*********************** Connected to MySQL server successfully ***********************")
  for statement in update_statements:
    cursor.execute(statement)
    conn.commit()

  cursor.close()
  conn.close()

else:
  logger.error("Some error occurred in between the process.")
  sys.exit()

logger.info("************************ Deleting older log files from GCS log directory ************************")
gcs_delete_old_logs(config.bucket_name, config.gcs_log_dir, 30)

logger.info("************************ Uploading log file to GCS log directory ************************")
upload_log_to_gcs(f"{config.log_dir}{log_file}", config.bucket_name, config.gcs_log_dir)