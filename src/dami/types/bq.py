from typing import Literal, Self
from pydantic import BaseModel, model_validator

# datetime is not allowed in BQ schema types
BQDataType = Literal[
    "STRING",
    "INTEGER",
    "FLOAT",
    "BOOLEAN",
    "TIMESTAMP",
    "DATE",
    "TIME",
    "RECORD",
]


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
