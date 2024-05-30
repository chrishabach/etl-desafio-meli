import polars as pl
import json
import logging

logger = logging.getLogger(__name__)

class DataValidator:
    def __init__(self, data_source: pl.DataFrame, config_path: str):
        self.data = data_source
        self.config_path = config_path
        self.source_name, self.expectations = self.load_config()

    def load_config(self):
        """Loads configuration including expectations and source name from a JSON file."""
        with open(self.config_path, 'r') as file:
            config = json.load(file)
        return config.get("source_name", "Unknown Source"), config["expectations"]

    def validate(self):
        """Executes all specified validations from the JSON file and checks if all are correct."""
        results = []
        for expectation in self.expectations:
            check_type = expectation['check']
            if check_type == 'columns_exist':
                result = self.check_columns_exist(expectation['columns'])
                results.append(result)
            elif check_type == 'no_duplicates':
                result = self.check_no_duplicates(expectation['columns'])
                results.append(result)
            elif check_type == 'no_duplicates_by_column':
                result = self.check_no_duplicates(expectation['columns'])
                results.append(result)
            elif check_type == 'no_nulls':
                result = self.check_no_nulls(expectation['columns'])
                results.append(result)
            elif check_type == 'no_negative_values':
                result = self.check_no_negative_values(expectation['columns'])
                results.append(result)               

        if not all(results):
            return False
        logging.info(f"All validations passed successfully for source '{self.source_name}'.")
        return True

    def check_columns_exist(self, columns):
        """Checks that all specified columns exist in the DataFrame."""
        missing_columns = [col for col in columns if col not in self.data.columns]
        if missing_columns:
            logging.error(f"Source '{self.source_name}': Missing columns: {missing_columns}")
            return False
        return True

    def check_no_duplicates_by_columns(self, columns):
        """Checks for duplicates in the specified columns."""
        for column in columns:
            if self.data.select(column).is_unique().sum() != self.data.height:
                logging.error(f"Source '{self.source_name}': Duplicates found in column: {column}")
                return False
        return True
    
    def check_no_duplicates(self, columns):
        """Checks for duplicates in combinated columns (supports composite key)."""
        if len(self.data.select(columns).unique()) != self.data.height:
            logging.error(f"Source '{self.source_name}': Duplicates found in columns: {columns}")
            return False
        return True

    def check_no_nulls(self, columns):
        """Checks for null values in the specified columns."""
        for column in columns:
            if self.data[column].null_count() > 0:
                logging.error(f"Source '{self.source_name}': Null values found in column: {column}")
                return False
        return True

    def check_no_negative_values(self, columns):
        """Checks that specified columns do not contain negative values."""
        for column in columns:
            if (self.data[column] < 0).any():
                logging.error(f"Source '{self.source_name}': Negative values found in column: {column}")
                return False
        return True