from dataclasses import dataclass
import io
from typing import cast

from google.cloud import bigquery as bq
import polars as pl

from dami.types.bq import BQDataType, BQField, BQTable

from loguru import logger


BQ_TYPE_TO_POLARS_DTYPE: dict[BQDataType, type[pl.DataType]] = {
    "STRING": pl.String,
    "INTEGER": pl.Int64,
    "FLOAT": pl.Float64,
    "BOOLEAN": pl.Boolean,
    "TIMESTAMP": pl.Datetime,
    "DATE": pl.Date,
    "TIME": pl.Time,
}


def _validate_field(bq_field: BQField, polars_dtype: pl.DataType) -> None:
    if bq_field.type != "RECORD":
        expected_dtype = BQ_TYPE_TO_POLARS_DTYPE[bq_field.type]
        actual_dtype = polars_dtype.__class__
        if expected_dtype != actual_dtype:
            raise TypeError(
                f"Field {bq_field.name} has incorrect dtype: "
                f"expected {expected_dtype}, got {actual_dtype}"
            )
    else:
        # RECORD type: validate sub-fields
        if not isinstance(polars_dtype, pl.Struct):
            raise TypeError(
                f"Field {bq_field.name} is of type RECORD but polars dtype is {polars_dtype}"
            )
        assert bq_field.fields is not None  # for type checker
        polars_field_dtype: dict[str, pl.DataType] = {
            field.name: cast(pl.DataType, field.dtype)
            for field in polars_dtype.fields
        }
        for sub_field in bq_field.fields:
            _validate_field(
                bq_field=sub_field,
                polars_dtype=polars_field_dtype[sub_field.name],
            )


@dataclass
class BQPolarsHandler:
    client: bq.Client

    @staticmethod
    def validate_df(df: pl.DataFrame, table: BQTable) -> None:
        for field in table.fields:
            if field.name not in df.columns:
                raise ValueError(f"DataFrame is missing required field: {field.name}")
            polars_dtype = df.schema[field.name]
            _validate_field(
                bq_field=field,
                polars_dtype=polars_dtype,
            )

    def insert_df(self, df: pl.DataFrame, table: BQTable) -> None:
        self.validate_df(df, table)
        # Write DataFrame to stream as parquet file; does not hit disk
        logger.info(
            f"Inserting DataFrame into BQ table {table.project}.{table.dataset}.{table.table}"
        )
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
