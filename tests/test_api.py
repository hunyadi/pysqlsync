import unittest
from urllib.parse import quote

from pysqlsync.connection import ConnectionParameters, ConnectionSSLMode
from pysqlsync.factory import get_dialect, get_parameters


class TestAPI(unittest.TestCase):
    def test_dialect_unavailable(self) -> None:
        engine = get_dialect("test")
        self.assertEqual(engine.name, "test")
        with self.assertRaises(RuntimeError):
            engine.create_connection(ConnectionParameters())
        with self.assertRaises(RuntimeError):
            engine.create_generator()

    def test_connection_string(self) -> None:
        host = "server.example.com"
        port = 2310
        username = "my+user@example.com"
        password = "<?Your:Strong@Pass/w0rd>"
        database = "database"
        url = f"postgresql://{quote(username, safe='')}:{quote(password, safe='')}@{host}:{port}/{database}"
        dialect, params = get_parameters(url)
        self.assertEqual(dialect, "postgresql")
        self.assertEqual(params.host, host)
        self.assertEqual(params.port, port)
        self.assertEqual(params.username, username)
        self.assertEqual(params.password, password)
        self.assertEqual(params.database, database)
        self.assertEqual(params.ssl, None)
        self.assertEqual(
            str(params), r"my%2Buser%40example.com@server.example.com:2310/database"
        )

    def test_connection_query_parameters(self) -> None:
        host = "server.example.com"
        port = 2310
        database = "database"
        url_prefix = f"postgresql://{host}:{port}/{database}"

        url = f"{url_prefix}?key=value"
        dialect, params = get_parameters(url)
        self.assertEqual(dialect, "postgresql")
        self.assertEqual(params.host, host)
        self.assertEqual(params.port, port)
        self.assertEqual(params.database, database)
        self.assertEqual(params.ssl, None)
        self.assertEqual(str(params), r"server.example.com:2310/database")

        url = f"{url_prefix}?ssl=verify-full"
        dialect, params = get_parameters(url)
        self.assertEqual(dialect, "postgresql")
        self.assertEqual(params.host, host)
        self.assertEqual(params.port, port)
        self.assertEqual(params.database, database)
        self.assertEqual(params.ssl, ConnectionSSLMode.verify_full)
        self.assertEqual(
            str(params), r"server.example.com:2310/database?ssl=verify-full"
        )


if __name__ == "__main__":
    unittest.main()
