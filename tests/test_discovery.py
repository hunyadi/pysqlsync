import unittest

import tests.tables as tables
from pysqlsync.base import ConnectionParameters, GeneratorOptions
from pysqlsync.factory import get_engine
from pysqlsync.formation.converter import (
    DataclassConverterOptions,
    NamespaceMapping,
    dataclass_to_table,
)
from pysqlsync.formation.discovery import Reflection
from pysqlsync.formation.object_types import QualifiedId

engine = get_engine("postgresql")


class TestDiscovery(unittest.IsolatedAsyncioTestCase):
    params: ConnectionParameters

    @property
    def parameters(self) -> ConnectionParameters:
        return ConnectionParameters(
            host="localhost",
            port=5432,
            username="levente.hunyadi",
            password=None,
            database="levente.hunyadi",
        )

    @property
    def options(self) -> GeneratorOptions:
        return GeneratorOptions(namespaces={tables: "public"})

    async def test_tables(self) -> None:
        async with engine.create_connection(self.parameters, self.options) as conn:
            await conn.drop_table(tables.NumericTable, ignore_missing=True)

            options = DataclassConverterOptions(
                namespaces=NamespaceMapping({tables: "public"})
            )
            table_def = dataclass_to_table(tables.NumericTable, options=options)
            await conn.execute(str(table_def))

            ref = Reflection(conn)
            table_ref = await ref.get_table(QualifiedId("public", "NumericTable"))

            await conn.drop_table(tables.NumericTable)

            self.assertEqual(table_def, table_ref)


if __name__ == "__main__":
    unittest.main()
