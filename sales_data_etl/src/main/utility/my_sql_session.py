import mysql.connector
import importlib

with open("/tmp/sales_data_etl/param/env.txt", "r") as file:
  env = file.read().strip()
config_module = f'param.{env}.config'
config = importlib.import_module(config_module)

def get_mysql_conn():
  """
    Returns a MySQL database connection.
  """
  conn = mysql.connector.connect(
    host = config.host,
    user = config.properties['user'],
    password = config.properties['password'],
    database = config.database_name
  )
  return conn