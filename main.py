# A simple db repl support .exit command
from typing import Union, Literal
import asyncio
import os
import sys

from aioconsole import ainput, aprint
import parse

from constants import *
from models import *


class PyDB:
    async def run(self) -> None:
        """use mem as storage, don't need to allocate mem like C, cuz we use Python which
        is dynamically allocate memory.
        table->row
        requirement doc: https://cstack.github.io/db_tutorial/parts/part3.html
        """
        if len(sys.argv) != 2:
            await aprint(f'Usage: python main.py yourdb.db')
            exit(ExitStatus.EXIT_FAILURE)
        
        self.table = await self.db_open(sys.argv[1])

        while True:
            await self.prompt()
            data = await ainput()
            if data.startswith('.'):
                res = await self.do_meta_command(data)
                if res == MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND:
                    await aprint(f'Unrecognized command {data}')
            else:
                res, statement = await self.prepare_statement(data)
                if res == PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT:
                    await aprint(f'Unrecognized keyword at start of {data}')
                    continue
                elif res == PrepareResult.PREPARE_SYNTAX_ERROR:
                    await aprint(f'Syntax error. Could not parse statement.')
                    continue

                res = await self.execute_statement(statement)
                if res == ExecuteResult.EXECUTE_SUCCESS:
                    await aprint('Executed.')
                elif res == ExecuteResult.EXECUTE_TABLE_FULL:
                    await aprint('Error: Table full.')
    
    async def prompt(self) -> None:
        """print a prompt on the stdout
        """
        await aprint(f'db > ', end='')
    
    async def do_meta_command(self, data: str) -> str:
        """processing the meta command

        Args:
            data (str): _description_

        Returns:
            str: _description_
        """
        if data == '.exit':
            await self.db_close()
            exit(ExitStatus.EXIT_SUCCESS)
        return MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND

    async def prepare_statement(self, data: str) -> Union[str, Statement]:
        """currently only support select/insert

        Args:
            data (str): _description_

        Returns:
            Union[str, Statement]: _description_
        """
        upperData = data.upper()
        try:
            if upperData.startswith('SELECT'):
                return PrepareResult.PREPARE_SUCCESS, Statement(
                    statement_type=StatementType.STATEMENT_SELECT
                )
            elif upperData.startswith('INSERT'):
                parse_res = parse.parse('insert {:d} {} {}', data)
                if not parse_res:
                    return PrepareResult.PREPARE_SYNTAX_ERROR, Statement(
                        statement_type=StatementType.STATEMENT_INSERT
                    )

                return PrepareResult.PREPARE_SUCCESS, Statement(
                    statement_type=StatementType.STATEMENT_INSERT,
                    row=Row(id=parse_res[0], username=parse_res[1], email=parse_res[2])
                )
            else:
                return PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT, Statement(
                    statement_type=StatementType.STATEMENT_INSERT
                )
        except ValidationError as ex:
            return PrepareResult.PREPARE_SYNTAX_ERROR, Statement(
                statement_type=StatementType.STATEMENT_INSERT
            )

    async def execute_statement(self, statement: Statement) -> Literal:
        """execute the statement

        Args:
            statement (Statement): _description_
        """
        if statement.statement_type == StatementType.STATEMENT_SELECT:
            return await self.execute_select(statement)
        elif statement.statement_type == StatementType.STATEMENT_INSERT:
            return await self.execute_insert(statement)

    async def execute_insert(self, statement: Statement) -> ExecuteResult:
        """insert a row into table

        Args:
            statement (Statement): _description_

        Returns:
            ExecuteResult: _description_
        """
        if self.table.num_rows >= TABLE_MAX_ROWS:
            return ExecuteResult.EXECUTE_TABLE_FULL
        
        # serialize the row into corresponding page
        await self.serialize_row(statement.row, await self.row_slot(self.table.num_rows))
        self.table.num_rows += 1

        return ExecuteResult.EXECUTE_SUCCESS
    
    async def execute_select(self, statement) -> Literal:
        """print all records in table

        Args:
            statement (_type_): _description_

        Returns:
            Literal: _description_
        """
        for row_idx in range(self.table.num_rows):
            page_idx, row_offset = await self.row_slot(row_idx)
            row = await self.deserialize_row(self.table.pager.pages[page_idx][row_offset:ROW_SIZE])
            await aprint(f'({row.id}, {row.username.strip()}, {row.email.strip()})')
        return ExecuteResult.EXECUTE_SUCCESS

    async def pager_open(self, filename: str) -> Pager:
        """file are composed by pages, so use pager represent
        the db file

        Args:
            filename (str): _description_

        Returns:
            Pager: _description_
        """
        try:
            fd = open(filename, 'a+')
            file_length = fd.seek(0, os.SEEK_END)
            return Pager(
                file_descriptor=fd,
                file_length=file_length,
                pages=[None] * TABLE_MAX_PAGES
            )
        except OSError as ex:
            await aprint("Unable to open file")
            exit(ExitStatus.EXIT_FAILURE)

    async def db_open(self, filename: str) -> Table:
        """_summary_

        Args:
            filename (str): _description_

        Returns:
            Table: _description_
        """
        pager = await self.pager_open(filename)
        num_rows = pager.file_length / ROW_SIZE

        return Table(
            pager=pager,
            num_rows=num_rows
        )

    async def db_close(self) -> None:
        """close d
        """
        num_full_pages = self.table.num_rows // PAGE_SIZE

        # persist full page into db file
        for pdx in range(num_full_pages):
            if self.table.pager.pages[pdx]:
                await self.pager_flush(pdx, PAGE_SIZE)
                self.table.pager.pages[pdx] = None

        # persist partial page into db file
        additional_rows = self.table.num_rows % ROWS_PER_PAGE
        if additional_rows:
            if self.table.pager.pages[num_full_pages]:
                await self.pager_flush(num_full_pages, additional_rows * ROW_SIZE)
                self.table.pager.pages[num_full_pages] = None

        # close file
        try:
            self.table.pager.file_descriptor.close()
        except IOError as ex:
            await aprint(f'Error closing db file {ex}')
            exit(ExitStatus.EXIT_FAILURE)

        # free pager, since we are exiting, python will automatically
        # free the corresponding mem
        self.table.pager.pages = None
        self.table.pager = None
        self.table = None

    async def serialize_row(self, row: Row, page_idx: int, row_offset: int) -> None:
        """serialize row into corresponding page

        Args:
            row (Row): _description_
            page_idx (int): _description_
            row_offset (int): _description_

        Returns:
            _type_: _description_
        """
        # each row must be stred into ROW_SIZE
        self.table.pager.pages[page_idx][row_offset:ROW_SIZE] = str(row)
        return str(row)

    async def deserialize_row(self, data: str) -> Row:
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

    async def row_slot(self, row_num: int) -> Union[int, int]:
        """get the page index and offset in the page

        Args:
            row_num (int): _description_

        Returns:
            Union[int, int]: page-index, row-offset
        """
        page_num = row_num // ROWS_PER_PAGE
        await self.get_page(page_num)
        offset = (row_num % ROWS_PER_PAGE) * ROW_SIZE
        return page_num-1, offset-1
    
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
        if not self.table.pager[page_num]:
            num_pages = self.table.pager.file_length // PAGE_SIZE

            # might save a partial page at the end of file
            if self.table.pager.file_length % PAGE_SIZE:
                num_pages += 1
            
            if page_num <= num_pages:
                self.table.pager.file_descriptor.seek(page_num * PAGE_SIZE, os.SEEK_SET)
                data = self.table.pager.file_descriptor.read(PAGE_SIZE)
                if not len(data):
                    await aprint(f'Error reading file: {len(data)}')
                    exit(ExitStatus.EXIT_FAILURE)
                self.table.pager[page_num] = Page(List(data))
            else:  # its a new page
                self.table.pager[page_num] = Page(page=[])

        return self.table.pager[page_num]
    
    async def pager_flush(self, page_idx: int, size: int) -> None:
        """flush data on page-idx into file

        Args:
            page_idx (int): _description_
            size (int): _description_
        """
        if not self.table.pager.pages[page_idx]:
            await aprint(f'Tried to flush null page')
            exit(ExitStatus.EXIT_FAILURE)
        
        offset = self.table.pager.file_descriptor.seek(page_idx * PAGE_SIZE, os.SEEK_SET)
        if offset == -1:
            await aprint(f'Error seeking: {page_idx * PAGE_SIZE}')
            exit(ExitStatus.EXIT_FAILURE)
        
        written = self.table.pager.file_descriptor.write(self.table.pager.pages[page_idx])
        if written == -1:
            await aprint(f'Error writing: {page_idx}')
            exit(ExitStatus.EXIT_FAILURE)

if __name__ == '__main__':
    py_db = PyDB()
    asyncio.run(py_db.run())
