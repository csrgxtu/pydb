# A simple db repl support .exit command
from typing import Union, Literal
import asyncio
import os

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
        self.table = Table(num_rows=0, rows=[])

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

    async def execute_insert(self, statement: Statement) -> Literal:
        """insert a row into table

        Args:
            statement (Statement): _description_

        Returns:
            Literal: _description_
        """
        self.table.num_rows += 1
        self.table.rows.append(statement.row)
        return ExecuteResult.EXECUTE_SUCCESS
    
    async def execute_select(self, statement) -> Literal:
        """print all records in table

        Args:
            statement (_type_): _description_

        Returns:
            Literal: _description_
        """
        for row in self.table.rows:
            await aprint(f'({row.id}, {row.username}, {row.email})')
        return ExecuteResult.EXECUTE_SUCCESS

    async def pager_open(self, filename: str) -> Pager:
        """open db file and build a pager

        Args:
            filename (str): _description_

        Returns:
            Pager: _description_
        """
        try:
            fd = os.open(filename, os.O_RDWR|os.O_CREAT)
            fd = open(filename, '+')
            file_length = fd.seek(0, os.SEEK_END)  # seek to end to get length
            pager = Pager(
                file_descriptor=fd,
                file_length=file_length,
                pages=[None]*TABLE_MAX_PAGES,
            )
            return pager
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
        return Table(num_rows=pager.file_length/ROW_SIZE, pager=pager)

    async def get_page(self, page_num: int) -> None:
        """if memory miss, then load page from db file into memory

        Args:
            page_num (int): _description_
        """
        if page_num > TABLE_MAX_PAGES:
            await aprint(f'Tried to fetch page number out of bounds. {TABLE_MAX_PAGES}')
            exit(ExitStatus.EXIT_FAILURE)

        # page cache miss, load from file
        if not self.table.pager.pages[page_num]:
            num_pages = self.table.pager.file_length / PAGE_SIZE
            # may have a partial page at the end of the file
            if self.table.pager.file_length % PAGE_SIZE:
                num_pages += 1

            if page_num <= num_pages:
                self.table.pager.file_descriptor.seek(
                    page_num * PAGE_SIZE, os.SEEK_SET
                )
                data = self.table.pager.file_descriptor.read(PAGE_SIZE)
                if not len(data):
                    await aprint(f'Error reading file: {self.filename}')
                    exit(ExitStatus.EXIT_FAILURE)
                
                # build data into rows within page
                for rd in range(0, len(data), ROW_SIZE):
                    await aprint('ToDo')
        
        return self.table.pager.pages[page_num]
    
    async def pager_flush(self, page_num: int, size: int) -> None:
        """flush rows in page into file

        Args:
            page_num (int): _description_
            size (int): _description_
        """
        if not self.table.pager.pages[page_num]:
            await aprint(f'Tried to flush null page')
            exit(ExitStatus.EXIT_FAILURE)
        
        # seek to corresponding offset in file for page num
        offset = self.table.pager.file_descriptor.seek(page_num * PAGE_SIZE, os.SEEK_SET)
        if not offset:
            await aprint(f'Error seeking: ')
            exit(ExitStatus.EXIT_FAILURE)
        
        self.table.pager.file_descriptor.write()

    async def serialize_row(self, row: Row) -> str:
        """serialize row into str

        Args:
            row (Row): _description_

        Returns:
            str: _description_
        """
        pass

    async def deserialize_row(self, data: str) -> Row:
        """deserialize data info Row obj

        Args:
            data (str): _description_

        Returns:
            Row: _description_
        """
        parse_res = parse.parse('{:.8}{:.32}{:.255}', data)
        if not parse_res:
            await aprint(f'Failed Deserialize Row: {data}')
            exit(ExitStatus.EXIT_FAILURE)

        return Row(
            id=parse_res[0],
            username=parse_res[1],
            email=parse_res[2]
        )


if __name__ == '__main__':
    py_db = PyDB()
    asyncio.run(py_db.run())
