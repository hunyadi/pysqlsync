import unittest
import uuid
from datetime import datetime

from pysqlsync.base import GeneratorOptions
from pysqlsync.formation.py_to_sql import EnumMode
from pysqlsync.model.id_types import QualifiedId
from tests import tables
from tests.params import (
    MSSQLBase,
    MySQLBase,
    OracleBase,
    PostgreSQLBase,
    TestEngineBase,
    disable_integration_tests,
)
from tests.timed_test import TimedAsyncioTestCase


@unittest.skipIf(disable_integration_tests(), "database tests are disabled")
class TestConnection(TestEngineBase, TimedAsyncioTestCase):
    @property
    def options(self) -> GeneratorOptions:
        return GeneratorOptions(namespaces={tables: None})

    async def test_connection(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            if self.engine.name == "oracle":
                await conn.execute(
                    'CREATE TABLE "DataTable" ("id" number PRIMARY KEY, "data" clob);'
                )
            else:
                await conn.execute(
                    'CREATE TABLE "DataTable" ("id" int PRIMARY KEY, "data" text);'
                )
            await conn.execute('DROP TABLE "DataTable";')

    async def test_upsert(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.create_objects([tables.DataTable])
            generator = conn.connection.generator
            statement = generator.get_dataclass_upsert_stmt(tables.DataTable)
            records = generator.get_dataclasses_as_records(
                tables.DataTable,
                [
                    tables.DataTable(1, "a"),
                    tables.DataTable(2, "b"),
                    tables.DataTable(3, "c"),
                ],
            )
            await conn.execute_all(statement, records)
            await conn.drop_objects()

    async def test_bulk_upsert(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.create_objects([tables.DataTable])
            generator = conn.connection.generator
            statement = generator.get_dataclass_upsert_stmt(tables.DataTable)
            for i in range(10):
                records = generator.get_dataclasses_as_records(
                    tables.DataTable,
                    [
                        tables.DataTable(i * 1000 + j, str(i * 1000 + j))
                        for j in range(1000)
                    ],
                )
                await conn.execute_all(statement, records)
            await conn.drop_objects()

    def get_user_data(self) -> list[tables.UserTable]:
        return [
            tables.UserTable(
                id=k,
                created_at=datetime.now(),
                updated_at=datetime.now(),
                deleted_at=datetime.now(),
                uuid=uuid.uuid4(),
                name="Dr. Levente Hunyadi",
                short_name="Levente",
                sortable_name="Hunyadi, Levente",
                homepage_url="https://hunyadi.info.hu",
            )
            for k in range(1, 10001)
        ]

    async def test_dataclass_insert(self) -> None:
        data = self.get_user_data()

        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.create_objects([tables.UserTable])
            value = await conn.query_one(int, 'SELECT COUNT(*) FROM "UserTable"')
            self.assertEqual(value, 0)
            await conn.insert_data(tables.UserTable, [])
            value = await conn.query_one(int, 'SELECT COUNT(*) FROM "UserTable"')
            self.assertEqual(value, 0)
            await conn.insert_data(tables.UserTable, data)
            value = await conn.query_one(int, 'SELECT COUNT(*) FROM "UserTable"')
            self.assertEqual(value, len(data))
            await conn.drop_objects()

    async def test_dataclass_upsert(self) -> None:
        data = self.get_user_data()

        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.create_objects([tables.UserTable])
            value = await conn.query_one(int, 'SELECT COUNT(*) FROM "UserTable"')
            self.assertEqual(value, 0)
            await conn.upsert_data(tables.UserTable, [])
            value = await conn.query_one(int, 'SELECT COUNT(*) FROM "UserTable"')
            self.assertEqual(value, 0)
            await conn.upsert_data(tables.UserTable, data)
            value = await conn.query_one(int, 'SELECT COUNT(*) FROM "UserTable"')
            self.assertEqual(value, len(data))
            await conn.drop_objects()

    async def test_rows_upsert(self) -> None:
        options = GeneratorOptions(
            namespaces={tables: None}, enum_mode=EnumMode.RELATION
        )
        async with self.engine.create_connection(self.parameters, options) as conn:
            converter = conn.connection.generator.converter
            catalog = converter.dataclasses_to_catalog([tables.EnumTable])
            await conn.execute(str(catalog))
            conn.connection.generator.state = catalog
            table = catalog.get_table(QualifiedId(None, tables.EnumTable.__name__))
            state_table = catalog.get_table(
                QualifiedId(None, tables.WorkflowState.__name__)
            )
            await conn.upsert_rows(
                table,
                field_types=(int, str, str),
                records=[
                    (1, "active", None),
                    (2, "inactive", None),
                    (3, "deleted", None),
                ],
            )
            await conn.upsert_rows(
                table,
                field_types=(int, str, str),
                records=[
                    (4, "active", None),
                    (5, "inactive", None),
                    (6, "inactive", None),
                ],
            )
            await conn.upsert_rows(
                table,
                field_types=(int, str, str),
                records=[
                    (7, "deleted", "active"),
                    (8, "deleted", "inactive"),
                    (9, "deleted", "deleted"),
                ],
            )
            self.assertEqual(
                await conn.query_one(int, 'SELECT COUNT(*) FROM "EnumTable"'), 9
            )
            self.assertEqual(
                await conn.query_one(int, 'SELECT COUNT(*) FROM "WorkflowState"'), 3
            )
            await conn.execute(table.drop_stmt())
            await conn.execute(state_table.drop_stmt())


class TestOracleConnection(OracleBase, TestConnection):
    def drop_table_stmt(self, table: QualifiedId) -> str:
        return (
            "BEGIN\n"
            f"    EXECUTE IMMEDIATE 'DROP TABLE {table}';\n"
            "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;\n"
            "END;"
        )

    async def asyncSetUp(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.execute(self.drop_table_stmt(QualifiedId(None, "DataTable")))
            await conn.execute(self.drop_table_stmt(QualifiedId(None, "EnumTable")))
            await conn.execute(self.drop_table_stmt(QualifiedId(None, "UserTable")))
            await conn.execute(self.drop_table_stmt(QualifiedId(None, "WorkflowState")))


class TestPostgreSQLConnection(PostgreSQLBase, TestConnection):
    async def asyncSetUp(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.execute('DROP TABLE IF EXISTS "DataTable";')
            await conn.execute('DROP TABLE IF EXISTS "EnumTable";')
            await conn.execute('DROP TABLE IF EXISTS "UserTable";')
            await conn.execute('DROP TABLE IF EXISTS "WorkflowState";')
            await conn.execute('DROP TYPE IF EXISTS "WorkflowState";')


class TestMSSQLConnection(MSSQLBase, TestConnection):
    async def asyncSetUp(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.execute('DROP TABLE IF EXISTS "DataTable";')
            await conn.execute('DROP TABLE IF EXISTS "EnumTable";')
            await conn.execute('DROP TABLE IF EXISTS "UserTable";')
            await conn.execute('DROP TABLE IF EXISTS "WorkflowState";')


class TestMySQLConnection(MySQLBase, TestConnection):
    async def asyncSetUp(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.execute('DROP TABLE IF EXISTS "DataTable";')
            await conn.execute('DROP TABLE IF EXISTS "EnumTable";')
            await conn.execute('DROP TABLE IF EXISTS "UserTable";')
            await conn.execute('DROP TABLE IF EXISTS "WorkflowState";')


del TestConnection

if __name__ == "__main__":
    unittest.main()
