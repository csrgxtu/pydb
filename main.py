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
        await self.serialize_row(statement.row, await self.row_slot())
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
            if self.table.rows[row_idx]:
                row = self.table.rows[row_idx]
            else:
                row = await self.get_row(row_idx)
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
                pages=[Page([])] * TABLE_MAX_PAGES
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
        for row_idx in range(self.table.num_rows):
            if self.table.rows[row_idx]:
                await self.row_flush(row_idx)
        self.table.file_descriptor.close()

    async def get_row(self, row_idx: int) -> Row:
        """get missing row from disk

        Args:
            row_idx (int): _description_

        Returns:
            Row: _description_
        """
        if row_idx > MAX_ROWS:
            await aprint(f'Tried to fetch row number out of bounds. {MAX_ROWS}')
            exit(ExitStatus.EXIT_FAILURE)
        
        offset = row_idx * ROW_SIZE
        self.table.file_descriptor.seek(offset, os.SEEK_SET)
        data = self.table.file_descriptor.read(ROW_SIZE)
        if not len(data):
            await aprint(f'Error reading file: {self.filename}')
            exit(ExitStatus.EXIT_FAILURE)
        self.table.rows[row_idx] = await self.deserialize_row(data)
        return self.table.rows[row_idx]

    async def row_flush(self, row_idx: int) -> None:
        """fluch a row into db file

        Args:
            row_idx (int): _description_
        """
        data = await self.serialize_row(self.table.rows[row_idx])
        offset = row_idx * ROW_SIZE
        self.table.file_descriptor.seek(offset, os.SEEK_SET)
        self.table.file_descriptor.write(data)

    async def serialize_row(self, row: Row, offset: int, page: str) -> str:
        """serialize row into str into corresponding page

        Args:
            row (Row): _description_

        Returns:
            str: _description_
        """
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

if __name__ == '__main__':
    py_db = PyDB()
    asyncio.run(py_db.run())
