import unittest

from pysqlsync.base import ConnectionParameters
from pysqlsync.factory import get_dialect


class TestImport(unittest.TestCase):
    def test_postgresql(self) -> None:
        engine = get_dialect("postgresql")
        self.assertEqual(engine.name, "postgresql")
        engine.create_connection(ConnectionParameters())
        engine.create_generator()

    def test_unavailable(self) -> None:
        engine = get_dialect("test")
        self.assertEqual(engine.name, "test")
        with self.assertRaises(RuntimeError):
            engine.create_connection(ConnectionParameters())
        with self.assertRaises(RuntimeError):
            engine.create_generator()


if __name__ == "__main__":
    unittest.main()