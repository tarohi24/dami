
from dataclasses import dataclass
import io

from google.cloud import bigquery as bq
import polars as pl

from dami.types.bq import BQDataType, BQTable

from loguru import logger




BQ_TYPE_TO_POLARS_DTYPE: dict[BQDataType, type[pl.DataType]] = {
    "STRING": pl.String,
    "INTEGER": pl.Int64,
    "FLOAT": pl.Float64,
    "BOOLEAN": pl.Boolean,
    "TIMESTAMP": pl.Datetime,
    "DATE": pl.Date,
    "TIME": pl.Time,
    "STRUCT": pl.Struct,
}





@dataclass
class BQPolarsHandler:
    client: bq.Client

    @staticmethod
    def validate_df(df: pl.DataFrame, table: BQTable) -> None:
        for field in table.fields:
            if field.name not in df.columns:
                raise ValueError(f"Missing column: {field.name}")
            # check data types
            expected_dtype = BQ_TYPE_TO_POLARS_DTYPE[field.type]
            actual_dtype = df[field.name].dtype.__class__
            if expected_dtype != actual_dtype:
                raise TypeError(
                    f"Column {field.name} has incorrect dtype: "
                    f"expected {expected_dtype}, got {actual_dtype}"
                )

    def insert_df(self, df: pl.DataFrame, table: BQTable) -> None:
        self.validate_df(df, table)
        # Write DataFrame to stream as parquet file; does not hit disk
        logger.info(f"Inserting DataFrame into BQ table {table.project}.{table.dataset}.{table.table}")
        logger.info(df.head())
        with io.BytesIO() as stream:
            df.write_parquet(stream)
            stream.seek(0)
            parquet_options = bq.ParquetOptions()
            parquet_options.enable_list_inference = True
            job = self.client.load_table_from_file(
                stream,
                destination=f"{table.project}.{table.dataset}.{table.table}",
                project=table.project,
                job_config=bq.LoadJobConfig(
                    source_format=bq.SourceFormat.PARQUET,
                    parquet_options=parquet_options,
                ),
            )
        res = job.result()  # Waits for the job to complete
        logger.info(res)

    def fetch_df(self, query: str) -> pl.DataFrame:
        raise NotImplementedError()
