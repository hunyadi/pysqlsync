import unittest

from pysqlsync.base import Parameters
from pysqlsync.factory import get_engine
from pysqlsync.formation.converter import dataclass_to_table
from pysqlsync.formation.discovery import Reflection
from pysqlsync.formation.object_types import QualifiedId
from tests.tables import *

engine = get_engine("postgresql")


class TestDiscovery(unittest.IsolatedAsyncioTestCase):
    params: Parameters

    def parameters(self) -> Parameters:
        return Parameters(
            host="localhost",
            port=5432,
            username="levente.hunyadi",
            password=None,
            database="levente.hunyadi",
        )

    async def test_tables(self) -> None:
        async with engine.create_connection(self.parameters()) as conn:
            table_def = dataclass_to_table(NumericTable, namespace="public")
            await conn.execute(str(table_def))

            ref = Reflection(conn)
            table_ref = await ref.get_table(QualifiedId("public", "NumericTable"))

            await conn.drop_table(NumericTable)

            self.assertEqual(table_def, table_ref)


if __name__ == "__main__":
    unittest.main()
