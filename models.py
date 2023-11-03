from typing import List
from pydantic import BaseModel, PositiveInt, NonNegativeInt, constr, ValidationError
from constants import *

class Row(BaseModel):
    id: PositiveInt
    username: constr(min_length=1, max_length=32)
    email: constr(min_length=1,max_length=255)

    def __repr__(self) -> str:
        return f'Row->id:{self.id},username:{self.username},email:{self.email}'

    def __str__(self) -> str:
        return f'{self.id:{0}{'>'}{COLUMN_ID_SIZE}}{self.username:{' '}{'<'}{COLUMN_USERNAME_SIZE}}{self.email:{' '}{'<'}{COLUMN_EMAIL_SIZE}}'

class Statement(BaseModel):
    statement_type: StatementType
    row: Row = None  # select statement dont have row currently

class Table(BaseModel):
    file_descriptor: PositiveInt
    file_length: PositiveInt
    num_rows: NonNegativeInt
    rows: List[Row]
