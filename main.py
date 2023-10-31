# A simple db repl support .exit command
import asyncio
from aioconsole import ainput, aprint

class PyDB:
    async def run(self) -> None:
        """start a repl and respond to .exit cmd
        requirement doc: https://cstack.github.io/db_tutorial/parts/part1.html
        """
        while True:
            await self.prompt()
            data = await ainput()
            if data == '.exit':
                break
            else:
                await aprint(f'Unrecognized command: {data}')
    
    async def prompt(self) -> None:
        """print a prompt on the stdout
        """
        await aprint(f'db > ', end='')


if __name__ == '__main__':
    py_db = PyDB()
    asyncio.run(py_db.run())
