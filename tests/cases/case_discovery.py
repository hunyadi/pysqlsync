import unittest

import tests.tables as tables
from pysqlsync.base import GeneratorOptions
from pysqlsync.formation.object_types import QualifiedId
from pysqlsync.formation.py_to_sql import (
    DataclassConverterOptions,
    NamespaceMapping,
    dataclass_to_table,
)
from tests.params import TestEngineBase


class TestDiscovery(unittest.IsolatedAsyncioTestCase, TestEngineBase):
    @property
    def options(self) -> GeneratorOptions:
        return GeneratorOptions(namespaces={tables: None})

    async def asyncSetUp(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.execute('DROP TABLE IF EXISTS "NumericTable";')
            await conn.execute('DROP TABLE IF EXISTS "Person";')
            await conn.execute('DROP TABLE IF EXISTS "Address";')

    async def test_table(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            options = DataclassConverterOptions(
                namespaces=NamespaceMapping({tables: None})
            )
            table_def = dataclass_to_table(tables.NumericTable, options=options)
            await conn.execute(str(table_def))

            ref = self.engine.create_explorer(conn)
            table_ref = await ref.get_table_meta(
                QualifiedId(None, tables.NumericTable.__name__)
            )

            await conn.execute(table_def.drop_stmt())

            self.assertMultiLineEqual(str(table_def), str(table_ref))

    async def test_relation(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            options = DataclassConverterOptions(
                namespaces=NamespaceMapping({tables: None})
            )
            address_def = dataclass_to_table(tables.Address, options=options)
            await conn.execute(str(address_def))
            person_def = dataclass_to_table(tables.Person, options=options)
            await conn.execute(str(person_def))

            ref = self.engine.create_explorer(conn)
            address_ref = await ref.get_table_meta(
                QualifiedId(None, tables.Address.__name__)
            )
            person_ref = await ref.get_table_meta(
                QualifiedId(None, tables.Person.__name__)
            )

            await conn.execute(person_def.drop_stmt())
            await conn.execute(address_def.drop_stmt())

            self.assertMultiLineEqual(str(address_def), str(address_ref))
            self.assertMultiLineEqual(str(person_def), str(person_ref))
