from typing import List
from pydantic import BaseModel, PositiveInt, NonNegativeInt, constr, ValidationError
from constants import *

class Row(BaseModel):
    id: PositiveInt
    username: constr(min_length=1, max_length=32)
    email: constr(min_length=1,max_length=255)

class Statement(BaseModel):
    statement_type: StatementType
    row: Row = None  # select statement dont have row currently

class Table(BaseModel):
    num_rows: NonNegativeInt
    rows: List[Row]