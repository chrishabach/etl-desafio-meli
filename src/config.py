import os

class Config:
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    DATA_DIR = os.path.join(BASE_DIR, "data")
    RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")
    PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed")
    LOG_DIR = os.path.join(BASE_DIR, "logs")
    LOGGER_CFG_PATH = os.path.join(BASE_DIR, 'logging.conf')
    EXPECTATION_DIR = os.path.join(LOG_DIR, "expectations")