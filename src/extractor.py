import polars as pl
import os
import json
import logging

logger = logging.getLogger(__name__)

class DataExtractor:
    def __init__(self, path_prints: str, path_taps: str, path_pays: str):
        self.path_prints = path_prints
        self.path_taps = path_taps
        self.path_pays = path_pays
        
    def _load_json_to_dataframe(self, file_path):
        # Verificar si el archivo existe
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            raise

        with open(file_path, 'r') as file:
            json_data = [json.loads(line) for line in file]
                    
        return pl.DataFrame(json_data)
    
    def _load_csv_to_dataframe(self, file_path):
        # Verificar si el archivo CSV existe
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")

        return pl.read_csv(file_path)
    
    def extract(self) -> tuple:
        pl_prints = self._load_json_to_dataframe(self.path_prints)
        pl_taps = self._load_json_to_dataframe(self.path_taps)
        pl_pays = self._load_csv_to_dataframe(self.path_pays)
        
        return pl_prints, pl_taps, pl_pays

