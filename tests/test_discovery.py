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
from pysqlsync.model.id_types import LocalId
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

            explorer = self.engine.create_explorer(conn)
            table_ref = await explorer.get_table_meta(
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

            explorer = self.engine.create_explorer(conn)
            address_ref = await explorer.get_table_meta(
                QualifiedId(current_namespace, tables.Address.__name__)
            )
            person_ref = await explorer.get_table_meta(
                QualifiedId(current_namespace, tables.Person.__name__)
            )

            await conn.execute(person_def.drop_stmt())
            await conn.execute(address_def.drop_stmt())

            self.assertMultiLineEqual(str(address_def), str(address_ref))
            self.assertMultiLineEqual(str(person_def), str(person_ref))

    async def test_formation(self) -> None:
        async with self.engine.create_connection(
            self.parameters, GeneratorOptions(namespaces={tables: "sample"})
        ) as conn:
            generator = conn.connection.generator
            create_stmt = generator.create(tables=[tables.UserTable])
            if create_stmt is None:
                self.fail()
            await conn.execute(create_stmt)

            catalog = conn.connection.generator.state
            create_ns = (
                catalog.namespaces["sample"]
                if "sample" in catalog.namespaces
                else catalog.namespaces[""]
            )

            explorer = self.engine.create_explorer(conn)
            discover_ns = await explorer.get_namespace_meta(LocalId("sample"))
            discover_stmt = str(discover_ns)

            self.assertMultiLineEqual(create_stmt, discover_stmt)
            self.assertEqual(create_ns, discover_ns)

            await conn.drop_objects()


class TestPostgreSQLDiscovery(PostgreSQLBase, TestDiscovery):
    async def get_current_namespace(self, conn: BaseContext) -> str:
        return await conn.query_one(str, "SELECT CURRENT_SCHEMA();")

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.drop_schema(LocalId("sample"))


class TestMySQLDiscovery(MySQLBase, TestDiscovery):
    async def get_current_namespace(self, conn: BaseContext) -> str:
        return await conn.query_one(str, "SELECT DATABASE();")

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.drop_schema(LocalId("sample"))


del TestDiscovery

if __name__ == "__main__":
    unittest.main()
