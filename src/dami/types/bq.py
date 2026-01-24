import datetime
import re
from typing import Literal, Self
from pydantic import BaseModel, field_validator, model_validator

import polars as pl

# datetime is not allowed in BQ schema types
BQDataType = Literal[
    "STRING",
    "INTEGER",
    "FLOAT",
    "BOOLEAN",
    "TIMESTAMP",
    "RECORD",
    "DATE",
    # the following types have not been supported in this project yet,
    # while BQ supports them.
    # "TIME",
]


PolarsTypeForBQ = (
    pl.String | pl.Int64 | pl.Float64 | pl.Boolean | pl.Datetime | pl.Struct | pl.Date
)

PythonTypeForBQ = str | int | float | bool | datetime.datetime | datetime.date


BQQuery = str


class BQField(BaseModel):
    name: str
    type: BQDataType
    mode: Literal["NULLABLE", "REQUIRED", "REPEATED"]
    description: str | None = None
    fields: list["BQField"] | None = None  # for RECORD type

    @model_validator(mode="after")
    def check_record_fields(self) -> Self:
        if self.type == "RECORD":
            if self.fields is None:
                raise ValueError(
                    f"Field {self.name} is of type RECORD but has no sub-fields defined."
                )
        else:
            if self.fields is not None:
                raise ValueError(
                    f"Field {self.name} is of type {self.type} but has sub-fields defined."
                )
        return self


class BQTable(BaseModel):
    project: str
    dataset: str
    table: str
    fields: list[BQField]

    @field_validator("table")
    def validate_table_name(cls, v: str) -> str:
        # table name must follow
        if not re.match(r"[a-z0-9-_]+", v):
            raise ValueError(
                f"Table name '{v}' is invalid. It must follow the pattern 'project.dataset.table' with allowed characters."
            )
        return v

    def get_bq_table_id(self) -> str:
        return f"`{self.project}.{self.dataset}.{self.table}`"

    @property
    def bq_schema(self) -> list[dict]:
        return [field.model_dump() for field in self.fields]
