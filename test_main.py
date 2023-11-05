import pytest
import pexpect


@pytest.mark.asyncio
async def test_pydb():
    # 1st, open the pydb
    client = pexpect.spawn('python main.py mydb.db')
    client.expect('db > ', timeout=3)

    # 2nd, insert 2 rows
    client.sendline('insert 1 cstack foo@bar.com')
    client.expect('Executed.', timeout=3)
    client.sendline('insert 2 bob bob@example.com')
    client.expect('Executed.', timeout=3)

    # 3rd, select all rows
    client.sendline('select')
    client.expect('(1, cstack, foo@bar.com)', timeout=3)
    client.expect('(2, bob, bob@example.com)', timeout=3)

    # 4th, insert a row with invalid field
    client.sendline('insert foo bar 1')
    client.expect('Syntax error. Could not parse statement.')

    # 5th, .exit cmd
    client.sendline('.exit')

    client.close()
