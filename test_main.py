import pytest
import pexpect
import uuid


@pytest.mark.asyncio
async def test_pydb():
    # 1st, open the pydb
    client = pexpect.spawn(f'python main.py /tmp/pydb-test-{uuid.uuid1()}.db')
    client.expect('db > ', timeout=3)

    # 2nd, first select with empty
    client.sendline('select')
    client.expect('Executed.', timeout=1)

    # 3rd, insert 2 rows
    client.sendline('insert 1 cstack foo@bar.com')
    client.expect('Executed.', timeout=1)
    client.sendline('insert 2 bob bob@example.com')
    client.expect('Executed.', timeout=1)

    # 4th, select all rows
    client.sendline('select')
    client.expect('(1, cstack, foo@bar.com)', timeout=3)
    client.expect('(2, bob, bob@example.com)', timeout=3)

    # 5th, insert a row with invalid field
    client.sendline('insert foo bar 1')
    client.expect('Syntax error. Could not parse statement.')

    # 6th, unrecognized cmd
    client.sendline('what')
    client.expect('Unrecognized keyword at start of what')

    # 7th, .exit cmd
    client.sendline('.exit')

    client.close()
