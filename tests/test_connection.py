import unittest

import tests.cases.case_connection as testcase
from tests.params import MySQLBase, PostgreSQLBase


class TestPostgreSQLConnection(testcase.TestConnection, PostgreSQLBase):
    async def asyncSetUp(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.execute('DROP TABLE IF EXISTS "DataTable";')
            await conn.execute('DROP TABLE IF EXISTS "EnumTable";')
            await conn.execute('DROP TABLE IF EXISTS "UserTable";')
            await conn.execute('DROP TABLE IF EXISTS "WorkflowState";')
            await conn.execute('DROP TYPE IF EXISTS "WorkflowState";')


class TestMySQLConnection(testcase.TestConnection, MySQLBase):
    async def asyncSetUp(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.execute('DROP TABLE IF EXISTS "DataTable";')
            await conn.execute('DROP TABLE IF EXISTS "EnumTable";')
            await conn.execute('DROP TABLE IF EXISTS "UserTable";')
            await conn.execute('DROP TABLE IF EXISTS "WorkflowState";')


if __name__ == "__main__":
    unittest.main()
