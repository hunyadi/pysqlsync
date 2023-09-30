import unittest

import tests.cases.case_discovery as testcase
from pysqlsync.base import BaseContext
from tests.params import MySQLBase, PostgreSQLBase


class TestPostgreSQLDiscovery(testcase.TestDiscovery, PostgreSQLBase):
    async def get_current_namespace(self, conn: BaseContext) -> str:
        return await conn.query_one(str, "SELECT CURRENT_SCHEMA();")


class TestMySQLDiscovery(testcase.TestDiscovery, MySQLBase):
    async def get_current_namespace(self, conn: BaseContext) -> str:
        return await conn.query_one(str, "SELECT DATABASE();")


if __name__ == "__main__":
    unittest.main()
