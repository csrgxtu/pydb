# A simple db repl support .exit command
from enum import Enum
from typing import Union, Literal
import asyncio
from aioconsole import ainput, aprint
import parse

class MetaCommandResult(Enum):
    META_COMMAND_SUCCESS = 0
    META_COMMAND_UNRECOGNIZED_COMMAND = 1

class PrepareResult(Enum):
    PREPARE_SUCCESS = 1
    PREPARE_UNRECOGNIZED_STATEMENT = 2
    PREPARE_SYNTAX_ERROR = 3

class StatementType(Enum):
    STATEMENT_INSERT = 1
    STATEMENT_SELECT = 2

class ExecuteResult(Enum):
    EXECUTE_SUCCESS = 1
    EXECUTE_TABLE_FULL = 2


class Row:
    """hard code table
    """
    id = None
    username = None
    email = None

class Statement:
    statement_type = StatementType.STATEMENT_INSERT  # default to insert
    row = Row()


class Table:
    num_rows = 0
    rows = []


class PyDB:
    async def run(self) -> None:
        """use mem as storage, don't need to allocate mem like C, cuz we use Python which
        is dynamically allocate memory.
        table->row
        requirement doc: https://cstack.github.io/db_tutorial/parts/part3.html
        """
        self.table = Table()

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
            exit(0)
        return MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND

    async def prepare_statement(self, data: str) -> Union[str, Statement]:
        """currently only support select/insert

        Args:
            data (str): _description_

        Returns:
            Union[str, Statement]: _description_
        """
        upperData, statement = data.upper(), Statement()
        if upperData.startswith('SELECT'):
            statement.statement_type = StatementType.STATEMENT_SELECT
        elif upperData.startswith('INSERT'):
            parse_res = parse.parse('insert {:d} {} {}', data)
            if not parse_res:
                return PrepareResult.PREPARE_SYNTAX_ERROR, statement
            
            row = Row()
            row.id, row.username, row.email = parse_res[0], parse_res[1], parse_res[2]
            statement.row = row
            statement.statement_type = StatementType.STATEMENT_INSERT
        else:
            return PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT, statement
        
        return PrepareResult.PREPARE_SUCCESS, statement

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

if __name__ == '__main__':
    py_db = PyDB()
    asyncio.run(py_db.run())
