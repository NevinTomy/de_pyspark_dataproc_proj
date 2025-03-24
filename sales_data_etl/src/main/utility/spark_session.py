from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from src.main.utility.logging_config import *

def spark_session():
    spark = SparkSession.builder \
      .appName("de_pyspark_dataproc_proj") \
      .config("spark.driver.extraClassPath", "/tmp/sales_data_etl/jars/mysql-connector-java-8.0.26.jar") \
      .getOrCreate()
    logger.info("Spark session successfully created!")
    return spark