import unittest

from pysqlsync.base import ClassRef, GeneratorOptions
from pysqlsync.formation.object_types import QualifiedId
from pysqlsync.formation.py_to_sql import NamespaceMapping, dataclass_to_table
from pysqlsync.model.id_types import LocalId
from tests import tables
from tests.model import user
from tests.params import (
    MSSQLBase,
    MySQLBase,
    OracleBase,
    PostgreSQLBase,
    TestEngineBase,
    disable_integration_tests,
)


@unittest.skipIf(disable_integration_tests(), "database tests are disabled")
class TestDiscovery(TestEngineBase, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        async with self.engine.create_connection(self.parameters) as conn:
            await conn.drop_table_if_exists(
                QualifiedId(None, tables.NumericTable.__name__)
            )
            await conn.drop_table_if_exists(QualifiedId(None, tables.Person.__name__))
            await conn.drop_table_if_exists(QualifiedId(None, tables.Address.__name__))
            await conn.drop_schema(LocalId("sample"))

    async def test_table(self) -> None:
        async with self.engine.create_connection(self.parameters) as conn:
            current_namespace = await conn.current_schema()
            options = conn.connection.generator.converter.options
            options.namespaces = NamespaceMapping({tables: current_namespace})

            table_def = dataclass_to_table(tables.NumericTable, options=options)
            await conn.execute(str(table_def))

            explorer = self.engine.create_explorer(conn)
            table_ref = await explorer.get_table(
                QualifiedId(current_namespace, tables.NumericTable.__name__)
            )

            await conn.execute(table_def.drop_stmt())

            self.assertMultiLineEqual(str(table_def), str(table_ref))

    async def test_relation(self) -> None:
        async with self.engine.create_connection(self.parameters) as conn:
            current_namespace = await conn.current_schema()
            options = conn.connection.generator.converter.options
            options.namespaces = NamespaceMapping({tables: current_namespace})

            address_def = dataclass_to_table(tables.Address, options=options)
            await conn.execute(str(address_def))
            person_def = dataclass_to_table(tables.Person, options=options)
            await conn.execute(str(person_def))

            explorer = self.engine.create_explorer(conn)
            address_ref = await explorer.get_table(
                QualifiedId(current_namespace, tables.Address.__name__)
            )
            person_ref = await explorer.get_table(
                QualifiedId(current_namespace, tables.Person.__name__)
            )

            await conn.execute(person_def.drop_stmt())
            await conn.execute(address_def.drop_stmt())

            self.assertMultiLineEqual(str(address_def), str(address_ref))
            self.assertMultiLineEqual(str(person_def), str(person_ref))

    async def test_formation(self) -> None:
        options = GeneratorOptions(namespaces={user: "sample"})
        async with self.engine.create_connection(self.parameters, options) as conn:
            generator = conn.connection.generator

            # create objects in the database
            execute_stmt = generator.create(tables=[user.UserTable])
            if execute_stmt is None:
                self.fail()
            await conn.execute(execute_stmt)

            # get reference schema
            generator.reset()
            create_stmt = generator.create(tables=[user.UserTable])
            if create_stmt is None:
                self.fail()

            catalog = conn.connection.generator.state
            create_ns = catalog.namespaces[
                "sample" if "sample" in catalog.namespaces else ""
            ]

            # discover actual schema
            explorer = self.engine.create_explorer(conn)
            discover_ns = await explorer.get_namespace(LocalId("sample"))
            discover_stmt = str(discover_ns)

            self.maxDiff = None
            self.assertMultiLineEqual(create_stmt, discover_stmt)
            self.assertEqual(create_ns, discover_ns)

            await conn.drop_objects()


# class TestOracleDiscovery(OracleBase, TestDiscovery):
#     pass


class TestPostgreSQLDiscovery(PostgreSQLBase, TestDiscovery):
    pass


class TestMSSQLDiscovery(MSSQLBase, TestDiscovery):
    pass


class TestMySQLDiscovery(MySQLBase, TestDiscovery):
    pass


del TestDiscovery

if __name__ == "__main__":
    unittest.main()
