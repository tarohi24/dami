from collections.abc import Mapping
from dataclasses import dataclass
import datetime
import io
from typing import cast

from google.cloud import bigquery as bq
import polars as pl

from dami.types.bq import (
    BQDataType,
    BQField,
    BQQuery,
    PolarsTypeForBQ,
    PythonTypeForBQ,
    BQTable,
)

from loguru import logger


BQ_TYPE_TO_POLARS_DTYPE: dict[BQDataType, type[PolarsTypeForBQ]] = {
    "STRING": pl.String,
    "INTEGER": pl.Int64,
    "FLOAT": pl.Float64,
    "BOOLEAN": pl.Boolean,
    "TIMESTAMP": pl.Datetime,
    "DATE": pl.Date,
}

PYTHON_TYPE_TO_BQ_TYPE: dict[type[PythonTypeForBQ], BQDataType] = {
    str: "STRING",
    int: "INTEGER",
    float: "FLOAT",
    bool: "BOOLEAN",
    datetime.datetime: "TIMESTAMP",
    datetime.date: "DATE",
}

BQQueryParameter = (
    bq.ArrayQueryParameter | bq.ScalarQueryParameter | bq.StructQueryParameter
)


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
            field.name: cast(pl.DataType, field.dtype) for field in polars_dtype.fields
        }
        for sub_field in bq_field.fields:
            _validate_field(
                bq_field=sub_field,
                polars_dtype=polars_field_dtype[sub_field.name],
            )


def _create_query_job_config_from_python(
    params: Mapping[str, PythonTypeForBQ],
) -> bq.QueryJobConfig:
    query_params: list[BQQueryParameter] = []
    for name, value in params.items():
        if isinstance(value, list):
            if len(value) == 0:
                raise ValueError(
                    f"Cannot create array query parameter {name} from empty list"
                )
            element_type = PYTHON_TYPE_TO_BQ_TYPE[type(value[0])]
            query_param = bq.ArrayQueryParameter(
                name=name,
                array_type=element_type,
                values=value,
            )
            query_params.append(query_param)
        elif isinstance(value, Mapping):
            raise NotImplementedError("Struct query parameters are not implemented yet")
        else:
            param_type = PYTHON_TYPE_TO_BQ_TYPE[type(value)]
            query_param = bq.ScalarQueryParameter(
                name=name,
                type_=param_type,
                value=value,
            )
            query_params.append(query_param)
    job_config = bq.QueryJobConfig(query_parameters=query_params)
    return job_config


def _generate_polars_schema(
    fields: list[BQField],
) -> dict[str, type[PolarsTypeForBQ] | pl.Struct]:
    schema: dict[str, type[PolarsTypeForBQ] | pl.Struct] = {}
    for field in fields:
        if field.type != "RECORD":
            polars_dtype = BQ_TYPE_TO_POLARS_DTYPE[field.type]
            schema[field.name] = polars_dtype
        else:
            assert field.fields is not None  # for type checker
            sub_schema = _generate_polars_schema(field.fields)
            schema[field.name] = pl.Struct(sub_schema)
    return schema


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
                    schema=table.bq_schema,
                ),
            )
        res = job.result()  # Waits for the job to complete
        logger.info(res)

    def fetch_df(
        self,
        query: BQQuery,
        table: BQTable,
        fields_to_fetch: list[str],
        params: Mapping[str, PythonTypeForBQ],
    ) -> pl.DataFrame:
        job_config = _create_query_job_config_from_python(params)
        field_mapping = {
            field.name: field for field in table.fields if field.name in fields_to_fetch
        }
        schema = _generate_polars_schema(
            [field_mapping[field] for field in fields_to_fetch]
        )
        job = self.client.query(query, job_config=job_config)
        res = job.to_arrow()
        df = cast(pl.DataFrame, pl.from_arrow(res, schema=schema))
        assert isinstance(df, pl.DataFrame)
        logger.info(f"Fetched {df.height} rows from BQ")
        return df

    def run_update_query(
        self,
        query: BQQuery,
        params: dict[str, PythonTypeForBQ],
    ) -> None:
        job_config = _create_query_job_config_from_python(params)
        job = self.client.query(query, job_config=job_config)
        job.result()  # Waits for the job to complete
        logger.info("completed update query")
