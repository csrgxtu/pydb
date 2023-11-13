from typing import TextIO, List, Union, Self
import os
from pydantic import BaseModel, PositiveInt, constr
import parse
from constants import *

from aioconsole import aprint

class Row(BaseModel):
    id: PositiveInt
    username: constr(min_length=1, max_length=32)
    email: constr(min_length=1,max_length=255)

    def __repr__(self) -> str:
        return f'Row->id:{self.id},username:{self.username},email:{self.email}'

    def __str__(self) -> str:
        return f'{self.id:{0}{'>'}{COLUMN_ID_SIZE}}{self.username:{''}{'<'}{COLUMN_USERNAME_SIZE}}{self.email:{''}{'<'}{COLUMN_EMAIL_SIZE}}'

    async def serialize_row(self) -> None:
        """serialize row into corresponding page

        Returns:
            _type_: _description_
        """
        # each row must be stred into ROW_SIZE
        return str(self)

    @classmethod
    async def deserialize_row(self, data: str) -> Self:
        """deserialize data info Row obj

        Args:
            data (str): _description_

        Returns:
            Row: _description_
        """
        str_format = '{{:.{}}}{{:.{}}}{{:.{}}}'
        parse_res = parse.parse(
            str_format.format(COLUMN_ID_SIZE, COLUMN_USERNAME_SIZE, COLUMN_EMAIL_SIZE),
            data
        )
        if not parse_res:
            await aprint(f'Failed Deserialize Row: {data}')
            exit(ExitStatus.EXIT_FAILURE)

        return Row(
            id=parse_res[0],
            username=parse_res[1],
            email=parse_res[2]
        )

class Statement(BaseModel):
    statement_type: StatementType
    row: Row = None  # select statement dont have row currently

class Page:
    def __init__(self, page: List[str]=['']*PAGE_SIZE) -> None:
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

    async def get_page(self, page_num: int) -> Page:
        """get page from current pager, if miss load it from file

        Args:
            page_num (int): _description_

        Returns:
            Page: _description_
        """
        if page_num > TABLE_MAX_PAGES:
            await aprint(f'Tried to fetch page number out of bounds. {page_num} > {TABLE_MAX_PAGES}')
            exit(ExitStatus.EXIT_FAILURE)

        # page not in mem, load it
        if not self.pages[page_num]:
            num_pages = self.file_length // PAGE_SIZE

            # might save a partial page at the end of file
            if self.file_length % PAGE_SIZE:
                num_pages += 1
            
            if num_pages and page_num <= num_pages:
                self.file_descriptor.seek(page_num * PAGE_SIZE, os.SEEK_SET)
                data = self.file_descriptor.read(PAGE_SIZE)
                if not len(data):
                    await aprint(f'Error reading file: {len(data)}')
                    exit(ExitStatus.EXIT_FAILURE)
                self.pages[page_num] = Page(list(data))
            else:  # its a new page
                self.pages[page_num] = Page()

        return self.pages[page_num]

    async def pager_flush(self, page_idx: int, size: int) -> None:
        """flush data on page-idx into file

        Args:
            page_idx (int): _description_
            size (int): _description_
        """
        if not self.pages[page_idx]:
            await aprint(f'Tried to flush null page')
            exit(ExitStatus.EXIT_FAILURE)
        
        offset = self.file_descriptor.seek(page_idx * PAGE_SIZE, os.SEEK_SET)
        if offset == -1:
            await aprint(f'Error seeking: {page_idx * PAGE_SIZE}')
            exit(ExitStatus.EXIT_FAILURE)
        
        written = self.file_descriptor.write(''.join(self.pages[page_idx].page))
        if written == -1:
            await aprint(f'Error writing: {page_idx}')
            exit(ExitStatus.EXIT_FAILURE)

    @classmethod
    async def pager_open(cls, filename: str) -> Self:
        """file are composed by pages, so use pager represent
        the db file

        Args:
            filename (str): _description_

        Returns:
            Pager: _description_
        """
        try:
            fd = open(filename, 'r+')
            file_length = fd.seek(0, os.SEEK_END)
            return Pager(
                file_descriptor=fd,
                file_length=file_length,
                pages=[None] * TABLE_MAX_PAGES
            )
        except FileNotFoundError as ex:
            fd = open(filename, 'w')
            fd.close()
            fd = open(filename, 'r+')
            return Pager(
                file_descriptor=fd,
                file_length=0,
                pages=[None] * TABLE_MAX_PAGES
            )
        except OSError as ex:
            await aprint("Unable to open file")
            exit(ExitStatus.EXIT_FAILURE)

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

    @classmethod
    async def db_open(cls, filename: str) -> Self:
        """_summary_

        Args:
            filename (str): _description_

        Returns:
            Table: _description_
        """
        pager = await Pager.pager_open(filename)
        num_rows = pager.file_length // ROW_SIZE

        return Table(
            pager=pager,
            num_rows=num_rows
        )

    async def db_close(self) -> None:
        """close database file
        """
        num_full_pages = self.num_rows // ROWS_PER_PAGE

        # persist full page into db file
        for pdx in range(num_full_pages):
            if self.pager.pages[pdx]:
                await self.pager.pager_flush(pdx, PAGE_SIZE)
                self.pager.pages[pdx] = None

        # persist partial page into db file
        additional_rows = self.num_rows % ROWS_PER_PAGE
        if additional_rows:
            if self.pager.pages[num_full_pages]:
                await self.pager.pager_flush(num_full_pages, additional_rows * ROW_SIZE)
                self.pager.pages[num_full_pages] = None

        # close file
        try:
            self.pager.file_descriptor.close()
        except IOError as ex:
            await aprint(f'Error closing db file {ex}')
            exit(ExitStatus.EXIT_FAILURE)

        # free pager, since we are exiting, python will automatically
        # free the corresponding mem
        self.pager.pages = None
        self.pager = None

class Cursor:
    def __init__(self, table: Table, row_num: int, end_of_table: bool) -> None:
        """_summary_

        Args:
            table (Table): _description_
            row_num (int): _description_
            end_of_table (bool): _description_
        """
        self.table = table
        self.row_num = row_num
        self.end_of_table = end_of_table
    
    @classmethod
    async def table_start(cls, table: Table) -> Self:
        """build cursor at start of table

        Args:
            table (Table): _description_
        
        Returns:
            Cursor: _description_
        """
        return Cursor(
            table=table,
            row_num=0,
            end_of_table=True if table.num_rows == 0 else False
        )
    
    @classmethod
    async def table_end(cls, table: Table) -> Self:
        """build cursor at end of table

        Args:
            table (Table): _description_
        
        Returns:
            Cursor: _description_
        """
        return Cursor(
            table=table,
            row_num=table.num_rows,
            end_of_table=True
        )
    
    async def cursor_advance(self) -> None:
        """move cursor one row forward
        """
        self.row_num += 1
        if self.row_num >= self.table.num_rows:
            self.end_of_table = True

    async def cursor_value(self) -> Union[int, int]:
        """get the page index and offset of row in the page pointed by cursor

        Returns:
            Union[int, int]: _description_
        """
        page_num = self.row_num // ROWS_PER_PAGE
        await self.table.pager.get_page(page_num)
        offset = (self.row_num % ROWS_PER_PAGE) * ROW_SIZE
        return page_num, offset
