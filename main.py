# A simple db repl support .exit command
from typing import Union, Literal
import asyncio
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
        
        self.table = await Table.db_open(sys.argv[1])

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
            await self.table.db_close()
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
        cursor = await Cursor.table_end(self.table)
        page_idx, row_offset = await cursor.cursor_value()
        serialized = await statement.row.serialize_row()
        self.table.pager.pages[page_idx].page[row_offset:row_offset+ROW_SIZE] = serialized
        self.table.num_rows += 1

        return ExecuteResult.EXECUTE_SUCCESS
    
    async def execute_select(self, statement) -> Literal:
        """print all records in table

        Args:
            statement (_type_): _description_

        Returns:
            Literal: _description_
        """
        cursor = await Cursor.table_start(self.table)

        while not cursor.end_of_table:
            page_idx, row_offset = await cursor.cursor_value()
            row = await Row.deserialize_row(''.join(self.table.pager.pages[page_idx].page[row_offset:row_offset+ROW_SIZE]))
            await aprint(f'({row.id}, {row.username.strip()}, {row.email.strip()})')
            await cursor.cursor_advance()

        return ExecuteResult.EXECUTE_SUCCESS

if __name__ == '__main__':
    py_db = PyDB()
    asyncio.run(py_db.run())
