import logging
import importlib
from datetime import datetime

with open("/tmp/sales_data_etl/param/env.txt", "r") as file:
  env = file.read().strip()
config_module = f"param.{env}.config"
config = importlib.import_module(config_module)

now = str(datetime.today().strftime('%Y%m%d_%H%M%S'))
log_file = f"{config.log_dir}de_sales_etl_{now}.log"
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(log_file), logging.StreamHandler()])
logger = logging.getLogger(__name__)