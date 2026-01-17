from typing import Literal
from pydantic import BaseModel


class BQField(BaseModel):
    name: str
    type: str
    mode: Literal["NULLABLE", "REQUIRED", "REPEATED"]
    description: str | None = None



class BQTable(BaseModel):
    project: str
    dataset: str
    table: str
    fields: list[BQField]

