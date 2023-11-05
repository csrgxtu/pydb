from typing import List, TextIO
from pydantic import BaseModel, PositiveInt, NonNegativeInt, constr
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

# class Table(BaseModel):
#     file_descriptor: Optional[TextIOWrapper]
#     file_length: NonNegativeInt
#     num_rows: NonNegativeInt
#     rows: List[Row] = []

class Table:
    def __init__(
            self, file_descriptor: TextIO, file_length: int,
            num_rows: int, rows: List[Row]
    ) -> None:
        """_summary_

        Args:
            file_descriptor (TextIO): _description_
            file_length (int): _description_
            num_rows (int): _description_
            rows (List[Row]): _description_
        """
        self.file_descriptor = file_descriptor
        self.file_length = file_length
        self.num_rows = num_rows
        self.rows = rows

