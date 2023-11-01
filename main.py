# A simple db repl support .exit command
from enum import Enum
from typing import Union
import asyncio
from aioconsole import ainput, aprint

class MetaCommandResult(Enum):
    META_COMMAND_SUCCESS = 0
    META_COMMAND_UNRECOGNIZED_COMMAND = 1

class PrepareResult(Enum):
    PREPARE_SUCCESS = 1
    PREPARE_UNRECOGNIZED_STATEMENT = 2

class StatementType(Enum):
    STATEMENT_INSERT = 1
    STATEMENT_SELECT = 2

class Statement:
    statement_type = StatementType.STATEMENT_INSERT  # default to insert


class PyDB:
    async def run(self) -> None:
        """start a repl and respond to .exit cmd
        requirement doc: https://cstack.github.io/db_tutorial/parts/part1.html
        """
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
                await self.execute_statement(statement)
                await aprint('Executed.')
    
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
            statement.statement_type = StatementType.STATEMENT_INSERT
        else:
            return PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT, statement
        
        return PrepareResult.PREPARE_SUCCESS, statement

    async def execute_statement(self, statement: Statement) -> None:
        """execute the statement

        Args:
            statement (Statement): _description_
        """
        if statement.statement_type == StatementType.STATEMENT_SELECT:
            await aprint('This is where we would do a select.')
        elif statement.statement_type == StatementType.STATEMENT_INSERT:
            await aprint('This is where we would do an insert.')


if __name__ == '__main__':
    py_db = PyDB()
    asyncio.run(py_db.run())
