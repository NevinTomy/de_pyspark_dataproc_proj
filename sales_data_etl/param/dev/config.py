from google.cloud import secretmanager
import os

def get_secret(secret_name):
    """ Fetches secret from Google secret manager """
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": secret_path})
    return response.payload.data.decode("UTF-8")


#GCS directories
project_id = "<gcp_proj_id>"
bucket_name = "<gcs_bucket_name>"
gcs_customer_datamart_directory = "customer_data_mart"
gcs_sales_datamart_directory = "sales_data_mart"
gcs_sales_partitioned_datamart_directory = "sales_partitioned_data_mart"
gcs_source_directory = "sales_data/"
gcs_error_directory = "sales_data_error/"
gcs_processed_directory = "sales_data_processed/"

#Log directory
gcs_log_dir = "logs/"
log_dir = "./logs/"

#Database credential
# MySQL database connection properties
database_name = "de_proj"
url = f"jdbc:mysql://<your_host>:3306/{database_name}"
properties = {
    "user": "gcp_etl_rw",
    "password": get_secret("mysql_pwd"),
    "driver": "com.mysql.cj.jdbc.Driver"
}
host = "<your_host>"

# Table name
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Required columns
mandatory_columns = ["customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost"]