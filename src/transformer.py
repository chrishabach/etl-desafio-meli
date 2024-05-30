import polars as pl
from data_validator import DataValidator
from config import Config
import logging
from exceptions import ValidateDataException

logger = logging.getLogger(__name__)

class DataTransformer:
    def __init__(self, pl_prints, pl_taps, pl_pays):
        self.pl_prints = pl_prints
        self.pl_taps = pl_taps
        self.pl_pays = pl_pays
        self.pl_prints_formater = None
        self.pl_taps_formater = None

    def _validate_data(self):
        """Validates data before proceeding with transformations."""
        # Validating prints data
        validator_prints = DataValidator(self.pl_prints_formater, Config.EXPECTATION_DIR + "/expected_prints.json")
        if not validator_prints.validate():
            raise ValidateDataException("Validation failed for PRINTS data")

        # Validating taps data
        validator_taps = DataValidator(self.pl_taps_formater, Config.EXPECTATION_DIR + "/expected_taps.json")
        if not validator_taps.validate():
            raise ValidateDataException("Validation failed for TAPS data")

        # Validating pays data
        validator_pays = DataValidator(self.pl_pays, Config.EXPECTATION_DIR + "/expected_pays.json")
        if not validator_pays.validate():
            raise ValidateDataException("Validation failed for PAYS data")

    def _normalize_struct_props(self, df, df_name):
        """Normalizes data structures for given DataFrame."""
        df = df.with_columns([
            df["event_data"].struct.field("position").alias("event_data_position"),
            df["event_data"].struct.field("value_prop").alias("event_data_value_prop"),
            pl.col("day").str.strptime(pl.Date, "%Y-%m-%d").alias("day")
        ])
        logger.info(f"{df_name} data normalized successfully.")
        return df.select(['day', 'event_data_position', 'event_data_value_prop', 'user_id'])

    def transform(self) -> pl.DataFrame:
        """Transforms data after successful validation."""

        self.pl_prints_formater = self._normalize_struct_props(self.pl_prints, "PRINTS")
        self.pl_taps_formater = self._normalize_struct_props(self.pl_taps, "TAPS")
        
        self._validate_data()
        
        # Agregar columna indicadora 'has_clicked' a pl_taps
        pl_taps = self.pl_taps_formater.with_column(pl.lit(1).alias("has_clicked"))

        # Join de los dataframes
        pl_prints_taps = self.pl_prints_formater.join(
            pl_taps, 
            on=['day', 'user_id', 'event_data_position', 'event_data_value_prop'], 
            how='left'
        ).with_column(
            (pl.col("day") - pl.duration(days=20)).alias("start_date")
        )
            
        pl_prints_left = pl_prints_taps.lazy()
        pl_prints_right = pl_prints_taps.lazy()

        pl_agg_value_prop = pl_prints_left.join(
            pl_prints_right,
            on=["user_id", "event_data_value_prop"],
            how="left"
        ).filter(
            (pl.col("day_right") >= pl.col("start_date")) &
            (pl.col("day_right") < pl.col("day"))
        ).groupby(["day", "user_id", "event_data_value_prop"]).agg([
            pl.count().alias("count_viewed_value_prop"),
            pl.sum("has_clicked_right").alias("count_clicked_value_prop")
        ])
        
        pl_agg_value_prop = pl_agg_value_prop.collect()

        # Join con los pagos
        pl_agg_prints_pays = pl_prints_taps.join(
            self.pl_pays,
            left_on=["user_id", "event_data_value_prop"],
            right_on=["user_id", "value_prop"],
            how="left"
        ).filter(
            (pl.col("pay_date").is_between(pl.col('start_date'), pl.col('day')))
        ).groupby(["day", "user_id", "event_data_value_prop"]).agg([
            pl.count().alias("count_total_pays"),
            pl.sum("total").alias("sum_total_pays")
        ])
        
        # Unir todos los resultados
        enriched_prints = pl_prints_taps.join(
            pl_agg_value_prop, 
            on=['day','user_id','event_data_value_prop'], 
            how='left'
        ).join(
            pl_agg_prints_pays, 
            on=['day', 'user_id', 'event_data_value_prop'], 
            how='left'
        )

        return enriched_prints