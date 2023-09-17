import unittest

import tests.case.discovery as testcase
from tests.params import MySQLBase, PostgreSQLBase


class TestPostgreSQLDiscovery(testcase.TestDiscovery, PostgreSQLBase):
    pass


class TestMySQLDiscovery(testcase.TestDiscovery, MySQLBase):
    pass


if __name__ == "__main__":
    unittest.main()
