from typing import Literal
from pydantic import BaseModel

# datetime is not allowed in BQ schema types
BQDataType = Literal[
    "STRING",
    "INTEGER",
    "FLOAT",
    "BOOLEAN",
    "TIMESTAMP",
    "DATE",
    "TIME",
    "STRUCT",
]


class BQField(BaseModel):
    name: str
    type: BQDataType
    mode: Literal["NULLABLE", "REQUIRED", "REPEATED"]
    description: str | None = None



class BQTable(BaseModel):
    project: str
    dataset: str
    table: str
    fields: list[BQField]

