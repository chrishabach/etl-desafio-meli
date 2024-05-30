import polars as pl
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, pl_transformed_data: pl.DataFrame, path_processed: str):
        self.pl_transformed_data = pl_transformed_data
        self.path_processed = path_processed
        self.pl_transformed_data_filter = self._select_columns(self.pl_transformed_data)
        
    def _select_columns(self, pl_transformed_data):
        # Usar Polars directamente para obtener el máximo día
        max_day_expr = pl_transformed_data.select(pl.max("day")).to_pandas().iloc[0,0]
        days_ago = max_day_expr - timedelta(days=6)

        # Filtrar y seleccionar las columnas necesarias sin convertir a pandas
        return pl_transformed_data.filter(pl.col('day') >= days_ago).select([
            'day', 
            'event_data_position', 
            'event_data_value_prop', 
            'user_id', 
            'has_clicked',
            'count_viewed_value_prop',
            'count_clicked_value_prop',
            'count_total_pays',
            'sum_total_pays'
        ])

    def load(self):
        self.pl_transformed_data_filter.write_csv(self.path_processed)
        logger.debug(f"Generated {self.pl_transformed_data_filter.shape[0]} rows to CSV successfully.")


