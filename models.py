from typing import List, IO, Any
from pydantic import BaseModel, PositiveInt, NonNegativeInt, constr, ValidationError
from constants import *

class Row(BaseModel):
    id: PositiveInt
    username: constr(min_length=1, max_length=32)
    email: constr(min_length=1,max_length=255)

class Statement(BaseModel):
    statement_type: StatementType
    row: Row = None  # select statement dont have row currently

class Page(BaseModel):
    rows: List[Row]

class Pager(BaseModel):
    file_descriptor: IO[Any]
    file_length: PositiveInt
    pages: List[Page]

class Table(BaseModel):
    num_rows: NonNegativeInt
    pager: Pager
