import unittest

import tests.tables as tables
from pysqlsync.base import GeneratorOptions
from pysqlsync.formation.converter import (
    DataclassConverterOptions,
    NamespaceMapping,
    dataclass_to_table,
)
from pysqlsync.formation.discovery import Reflection
from pysqlsync.formation.object_types import QualifiedId
from tests.params import TestEngineBase


class TestDiscovery(unittest.IsolatedAsyncioTestCase, TestEngineBase):
    @property
    def options(self) -> GeneratorOptions:
        return GeneratorOptions(namespaces={tables: None})

    async def asyncSetUp(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.execute('DROP TABLE IF EXISTS "NumericTable";')

    async def test_tables(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            options = DataclassConverterOptions(
                namespaces=NamespaceMapping({tables: None})
            )
            table_def = dataclass_to_table(tables.NumericTable, options=options)
            await conn.execute(str(table_def))

            ref = Reflection(conn)
            table_ref = await ref.get_table(QualifiedId(None, "NumericTable"))

            await conn.drop_table(tables.NumericTable)

            self.assertMultiLineEqual(str(table_def), str(table_ref))
