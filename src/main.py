import os
import logging
import logging.config
from config import Config

# Cambiar el directorio de trabajo a la raíz del proyecto
os.chdir( os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

logging.config.fileConfig(Config.LOGGER_CFG_PATH)
root_logger = logging.getLogger()
root_logger.propagate = True

if __name__ == "__main__":
    from etl import etl_pipeline
    etl_pipeline()