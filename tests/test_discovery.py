import abc
import unittest

import tests.tables as tables
from pysqlsync.base import BaseContext, GeneratorOptions
from pysqlsync.formation.object_types import QualifiedId
from pysqlsync.formation.py_to_sql import (
    DataclassConverterOptions,
    NamespaceMapping,
    dataclass_to_table,
)
from tests.params import MySQLBase, PostgreSQLBase, TestEngineBase


class TestDiscovery(TestEngineBase, unittest.IsolatedAsyncioTestCase):
    @property
    def options(self) -> GeneratorOptions:
        return GeneratorOptions(namespaces={tables: None})

    @abc.abstractmethod
    async def get_current_namespace(self, conn: BaseContext) -> str:
        ...

    async def asyncSetUp(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.execute('DROP TABLE IF EXISTS "NumericTable";')
            await conn.execute('DROP TABLE IF EXISTS "Person";')
            await conn.execute('DROP TABLE IF EXISTS "Address";')

    async def test_table(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            current_namespace = await self.get_current_namespace(conn)
            options = DataclassConverterOptions(
                namespaces=NamespaceMapping({tables: current_namespace})
            )
            table_def = dataclass_to_table(tables.NumericTable, options=options)
            await conn.execute(str(table_def))

            ref = self.engine.create_explorer(conn)
            table_ref = await ref.get_table_meta(
                QualifiedId(current_namespace, tables.NumericTable.__name__)
            )

            await conn.execute(table_def.drop_stmt())

            self.assertMultiLineEqual(str(table_def), str(table_ref))

    async def test_relation(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            current_namespace = await self.get_current_namespace(conn)
            options = DataclassConverterOptions(
                namespaces=NamespaceMapping({tables: current_namespace})
            )
            address_def = dataclass_to_table(tables.Address, options=options)
            await conn.execute(str(address_def))
            person_def = dataclass_to_table(tables.Person, options=options)
            await conn.execute(str(person_def))

            ref = self.engine.create_explorer(conn)
            address_ref = await ref.get_table_meta(
                QualifiedId(current_namespace, tables.Address.__name__)
            )
            person_ref = await ref.get_table_meta(
                QualifiedId(current_namespace, tables.Person.__name__)
            )

            await conn.execute(person_def.drop_stmt())
            await conn.execute(address_def.drop_stmt())

            self.assertMultiLineEqual(str(address_def), str(address_ref))
            self.assertMultiLineEqual(str(person_def), str(person_ref))


class TestPostgreSQLDiscovery(PostgreSQLBase, TestDiscovery):
    async def get_current_namespace(self, conn: BaseContext) -> str:
        return await conn.query_one(str, "SELECT CURRENT_SCHEMA();")


class TestMySQLDiscovery(MySQLBase, TestDiscovery):
    async def get_current_namespace(self, conn: BaseContext) -> str:
        return await conn.query_one(str, "SELECT DATABASE();")


del TestDiscovery

if __name__ == "__main__":
    unittest.main()
