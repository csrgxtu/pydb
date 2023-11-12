from typing import TextIO, List
from pydantic import BaseModel, PositiveInt, constr
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

class Page:
    def __init__(self, page: List[str]) -> None:
        """page should be a continous memory in C, in Python we use list of char instead of 
        str as a page, due to str is immutable which will need constantly reallocation.

        Args:
            page (List[str]): each page is represented by a list of char
        """
        self.page = page
class Pager:
    def __init__(self, file_descriptor: TextIO, file_length: int, pages: List[Page]) -> None:
        """_summary_

        Args:
            file_descriptor (TextIO): _description_
            file_length (int): _description_
            pages (List[Page]): refer Page
        """
        self.file_descriptor = file_descriptor
        self.file_length = file_length
        self.pages = pages

class Table:
    def __init__(
            self, pager: Pager, num_rows: int
    ) -> None:
        """_summary_

        Args:
            pager (Pager): _description_
            num_rows (int): _description_
        """
        self.pager = pager
        self.num_rows = num_rows

