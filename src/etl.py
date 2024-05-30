from abc import ABC, abstractmethod
import os
import sys
from typing import Any
import logging
import logging.config
import time
from config import Config
from exceptions import (
    ExtractProcessException,
    LoadProcessException,
    TransformProcessException,
)

logger = logging.getLogger(__name__)

class ETL(ABC):
    """Abstract base class for ETL (Extract, Transform, Load) process."""

    def __init__(self, path_prints: str, path_taps: str, path_pays: str, path_processed: str,) -> None:
        self.path_prints = path_prints
        self.path_taps = path_taps
        self.path_pays = path_pays
        self.path_processed = path_processed
        self.pl_prints = None
        self.pl_taps = None
        self.pl_pays = None
        self.pl_transformed_data = None

    @abstractmethod
    def extract(self) -> None:
        """Extract data from the specified sources."""
        raise NotImplementedError("Extract method must be implemented by subclasses")
    
    @abstractmethod
    def transform(self) -> None:
        """Transform the extracted data."""
        raise NotImplementedError("Transform method must be implemented by subclasses")

    @abstractmethod
    def load(self) -> Any:
        """Load the transformed data into the process folder."""
        raise NotImplementedError("Load method must be implemented by subclasses")
    
class PropsETL(ETL):
    """ETL process for extracting, transforming, and loading properties data."""

    def extract(self) -> None:
        """Extract data from raw sources."""
        from extractor import DataExtractor

        extractor = DataExtractor(self.path_prints, self.path_taps, self.path_pays)
        try:
            self.pl_prints, self.pl_taps, self.pl_pays = extractor.extract()
            logger.info("Data extracted successfully.")
        except Exception:
            raise ExtractProcessException("Could not extract data")
    
    def transform(self) -> None:
        """Transform the extracted data."""
        from transformer import DataTransformer
        transformer = DataTransformer(self.pl_prints, self.pl_taps, self.pl_pays)

        try:
            self.pl_transformed_data = transformer.transform()
            logger.info("Data transformed successfully.")
        except Exception:
              raise TransformProcessException("Could not transform data")
    
    def load(self) -> Any:
        """Load the transformed data."""
        from loader import DataLoader
        loader = DataLoader(self.pl_transformed_data, self.path_processed)

        try:
            self.pl_transformed_data = loader.load()
            logger.info("Data loader successfully.")
        except Exception as e:
            raise LoadProcessException("Failed to write DataFrame to CSV")        
        
def etl_pipeline():
    init_time = time.time()
    logger.info("ETL is starting...")

    path_prints = os.path.join(Config.RAW_DATA_DIR, "prints.json")
    path_taps = os.path.join(Config.RAW_DATA_DIR, "taps.json")
    path_pays = os.path.join(Config.RAW_DATA_DIR, "pays.csv")
    path_processed = os.path.join(Config.PROCESSED_DATA_DIR, "result_dataset.csv")

    # ETL Process
    try:
        etl = PropsETL(path_prints, path_taps, path_pays, path_processed)
        etl.extract()
        etl.transform()
        etl.load()

        end_time = round(time.time() - init_time, 2)
        logger.info(
            f"ETL finished successfully. Time elapsed: {end_time} seconds"
        )
    except ExtractProcessException as e:
        logger.error(
            f"Error Extracting Data : {e}",
            exc_info=sys.exc_info(),
        )
    except TransformProcessException as e:
        logger.error(
            f"Error Transform Data : {e}",
            exc_info=sys.exc_info(),
        )
    except LoadProcessException as e:
        logger.error(
            f"Error Load Data : {e}",
            exc_info=sys.exc_info(),
        )