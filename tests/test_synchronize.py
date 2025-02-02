import copy
import random
import unittest
from dataclasses import dataclass
from io import BytesIO

from strong_typing.inspection import DataclassInstance, dataclass_fields

from pysqlsync.base import BaseContext, ClassRef, GeneratorOptions
from pysqlsync.data.exchange import TextReader, TextWriter, fields_to_types
from pysqlsync.data.generator import random_objects
from pysqlsync.formation.inspection import get_entity_types
from pysqlsync.formation.mutation import MutatorOptions
from pysqlsync.formation.object_types import FormationError, Table
from pysqlsync.formation.py_to_sql import ArrayMode, EnumMode, StructMode
from pysqlsync.model.id_types import LocalId, QualifiedId
from pysqlsync.model.key_types import DEFAULT
from pysqlsync.model.properties import get_primary_key_name_type
from tests import tables
from tests.model import event, school, user
from tests.params import (
    MSSQLBase,
    MySQLBase,
    OracleBase,
    PostgreSQLBase,
    TestEngineBase,
    configure,
    has_env_var,
)
from tests.util import reuse_guard

if __name__ == "__main__":
    configure()


@dataclass
class TestOptions:
    enum_mode: EnumMode
    array_mode: ArrayMode
    struct_mode: StructMode


@unittest.skipUnless(has_env_var("INTEGRATION"), "database tests are disabled")
class TestSynchronize(TestEngineBase, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.drop_schema(LocalId("sample"))
            await conn.drop_schema(LocalId("event"))
            await conn.drop_schema(LocalId("school"))
            await conn.drop_table_if_exists(QualifiedId(None, user.UserTable.__name__))

    @property
    def options(self) -> GeneratorOptions:
        return GeneratorOptions(
            namespaces={tables: "sample", event: "event", school: "school", user: None},
            foreign_constraints=False,
        )

    async def test_create_schema(self) -> None:
        options = GeneratorOptions(
            namespaces={tables: "sample", event: "event", school: "school", user: None},
            foreign_constraints=True,
            synchronization=MutatorOptions(
                allow_drop_namespace=False, allow_drop_table=False
            ),
        )
        async with self.engine.create_connection(self.parameters, options) as conn:
            explorer = self.engine.create_explorer(conn)
            self.assertFalse(conn.connection.generator.state)
            await explorer.synchronize(modules=[tables, event, school, user])
            self.assertTrue(conn.connection.generator.state)

    async def test_discover_schema(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            explorer = self.engine.create_explorer(conn)
            await explorer.synchronize(module=tables)
            state = copy.deepcopy(conn.connection.generator.state)

        async with self.engine.create_connection(self.parameters, self.options) as conn:
            explorer = self.engine.create_explorer(conn)
            await explorer.discover(module=tables)

            if self.engine.name != "postgresql":
                return

            for ns in conn.connection.generator.state.namespaces.values():
                ns.tables.sort()
            self.assertEqual(state, conn.connection.generator.state)

    async def test_update_schema(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            explorer = self.engine.create_explorer(conn)

            await explorer.synchronize(module=tables)
            state = copy.deepcopy(conn.connection.generator.state)

            await explorer.synchronize(module=tables)
            self.assertEqual(state, conn.connection.generator.state)

    async def test_modes(self) -> None:
        for combination in [
            TestOptions(EnumMode.TYPE, ArrayMode.ARRAY, StructMode.TYPE),
            TestOptions(EnumMode.TYPE, ArrayMode.JSON, StructMode.JSON),
            TestOptions(EnumMode.INLINE, ArrayMode.JSON, StructMode.JSON),
            TestOptions(EnumMode.RELATION, ArrayMode.ARRAY, StructMode.TYPE),
            TestOptions(EnumMode.RELATION, ArrayMode.JSON, StructMode.JSON),
            TestOptions(EnumMode.CHECK, ArrayMode.ARRAY, StructMode.TYPE),
            TestOptions(EnumMode.CHECK, ArrayMode.JSON, StructMode.JSON),
        ]:
            options = GeneratorOptions(
                enum_mode=combination.enum_mode,
                array_mode=combination.array_mode,
                struct_mode=combination.struct_mode,
                namespaces={tables: "sample"},
            )
            try:
                self.engine.create_generator(options)
            except FormationError:
                # skip unsupported parameter combinations
                continue

            with self.subTest(combination=combination):
                async with self.engine.create_connection(
                    self.parameters, options
                ) as conn:
                    await conn.drop_schema(LocalId("sample"))
                    explorer = self.engine.create_explorer(conn)
                    await explorer.synchronize(module=tables)

    async def get_rows(self, conn: BaseContext, table: Table) -> int:
        count = await conn.query_one(int, f"SELECT COUNT(*) FROM {table.name}")
        return count

    async def test_insert_update_delete_rows_plain(self) -> None:
        await self.insert_update_delete_rows_plain([tables.UniqueTable])

    async def test_insert_update_delete_rows_relation(self) -> None:
        await self.insert_update_delete_rows_relation([tables.UniqueTable])

    async def insert_update_delete_rows_plain(
        self, exclude: list[type[DataclassInstance]]
    ) -> None:
        await self.insert_update_delete_rows(self.options, exclude)

    async def insert_update_delete_rows_relation(
        self, exclude: list[type[DataclassInstance]]
    ) -> None:
        options = GeneratorOptions(
            namespaces={tables: "sample", event: "event", school: "school", user: None},
            foreign_constraints=False,
            enum_mode=EnumMode.RELATION,
        )
        await self.insert_update_delete_rows(options, exclude)

    async def insert_update_delete_rows(
        self, options: GeneratorOptions, exclude: list[type[DataclassInstance]]
    ) -> None:
        async with self.engine.create_connection(self.parameters, options) as conn:
            explorer = self.engine.create_explorer(conn)
            await explorer.synchronize(module=tables)

        async with self.engine.create_connection(self.parameters, options) as conn:
            explorer = self.engine.create_explorer(conn)
            await explorer.synchronize(module=tables)
            entity_types = get_entity_types([tables])

            for entity_type in entity_types:
                if entity_type in exclude:
                    continue

                with self.subTest(entity=entity_type.__name__):
                    entities = random_objects(entity_type, 100)

                    # randomize order of columns in tabular file
                    field_names = [
                        field.name for field in dataclass_fields(entity_type)
                    ]

                    # generate random data and write records
                    random.shuffle(field_names)
                    field_mapping = {name: f"value.{name}" for name in field_names}
                    with BytesIO() as stream:
                        writer = TextWriter(stream, entity_type, field_mapping)
                        writer.write_objects(entities)
                        data = stream.getvalue()

                    # read and parse data into records
                    field_names.sort()
                    field_mapping = {name: f"value.{name}" for name in field_names}
                    with BytesIO(data) as stream:
                        reader = TextReader(
                            stream, fields_to_types(entity_type, field_mapping)
                        )
                        field_labels, field_types = reader.columns, reader.field_types
                        rows = reader.read_records()

                    # use field order as in data file
                    field_mapping = {value: key for key, value in field_mapping.items()}
                    field_labels = tuple(field_mapping[label] for label in field_labels)

                    # truncate table
                    table = conn.get_table(entity_type)
                    await conn.execute(f"DELETE FROM {table.name}")
                    self.assertEqual(await self.get_rows(conn, table), 0)

                    # insert data in database table
                    await conn.insert_rows(
                        table,
                        field_names=field_labels,
                        field_types=field_types,
                        records=reuse_guard(row for row in rows),
                    )
                    self.assertEqual(await self.get_rows(conn, table), len(entities))

                    # update data in database table
                    await conn.upsert_rows(
                        table,
                        field_names=field_labels,
                        field_types=field_types,
                        records=reuse_guard(row for row in rows),
                    )
                    self.assertEqual(await self.get_rows(conn, table), len(entities))

                    # delete data from database table
                    primary_name, primary_type = get_primary_key_name_type(entity_type)
                    keys = [getattr(entity, primary_name) for entity in entities]
                    await conn.delete_rows(table, primary_type, keys)
                    self.assertEqual(await self.get_rows(conn, table), 0)

    async def test_identity_dataclass(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            cls = tables.UniqueTable
            table_name = conn.connection.generator.get_qualified_id(ClassRef(cls))

            await conn.create_objects([cls])

            # insert 1 records, then insert 2 records
            await conn.insert_data(cls, [cls(id=DEFAULT, unique="value 1")])
            await conn.insert_data(
                cls,
                [
                    cls(id=DEFAULT, unique="value 2"),
                    cls(id=DEFAULT, unique="value 3"),
                ],
            )
            count = await conn.query_one(int, f"SELECT COUNT(*) FROM {table_name}")
            self.assertEqual(count, 3)

            # update 2 records and insert 3 records
            # caveat: Microsoft SQL ignores explicit value for IDENTITY column when inserting new record
            # caveat: Oracle increments `nextval` even if a match is found
            await conn.upsert_data(
                cls,
                [
                    cls(id=2, unique="value 2"),
                    cls(id=3, unique="value 3"),
                ],
            )
            await conn.insert_data(
                cls,
                [
                    cls(id=DEFAULT, unique="value 4"),
                    cls(id=DEFAULT, unique="value 5"),
                    cls(id=DEFAULT, unique="value 6"),
                ],
            )
            count = await conn.query_one(int, f"SELECT COUNT(*) FROM {table_name}")
            self.assertEqual(count, 6)

            # delete 1 record
            await conn.delete_data(cls, [6])
            count = await conn.query_one(int, f"SELECT COUNT(*) FROM {table_name}")
            self.assertEqual(count, 5)

            await conn.drop_objects()

    async def enum_migration(self, options: GeneratorOptions) -> None:
        async with self.engine.create_connection(self.parameters, options) as conn:
            explorer = self.engine.create_explorer(conn)
            await explorer.synchronize(module=tables)

        options = GeneratorOptions(
            array_mode=ArrayMode.RELATION,
            enum_mode=EnumMode.RELATION,
            namespaces={tables: "sample"},
        )
        async with self.engine.create_connection(self.parameters, options) as conn:
            explorer = self.engine.create_explorer(conn)
            await explorer.synchronize(module=tables)


@unittest.skipUnless(has_env_var("ORACLE"), "Oracle tests are disabled")
class TestOracleSynchronize(OracleBase, TestSynchronize):
    pass


@unittest.skipUnless(has_env_var("POSTGRESQL"), "PostgreSQL tests are disabled")
class TestPostgreSQLSynchronize(PostgreSQLBase, TestSynchronize):
    async def test_insert_update_delete_rows_relation(self) -> None:
        await self.insert_update_delete_rows_relation(
            [tables.EnumArrayTable, tables.EnumSetTable, tables.UniqueTable]
        )

    async def test_enum_migration(self) -> None:
        options = GeneratorOptions(
            enum_mode=EnumMode.TYPE,
            namespaces={tables: "sample"},
        )
        await self.enum_migration(options)


@unittest.skipUnless(has_env_var("MSSQL"), "Microsoft SQL tests are disabled")
class TestMSSQLSynchronize(MSSQLBase, TestSynchronize):
    pass


@unittest.skipUnless(has_env_var("MYSQL"), "MySQL tests are disabled")
class TestMySQLSynchronize(MySQLBase, TestSynchronize):
    async def test_enum_migration(self) -> None:
        options = GeneratorOptions(
            enum_mode=EnumMode.INLINE,
            namespaces={tables: "sample"},
        )
        await self.enum_migration(options)


del TestSynchronize

if __name__ == "__main__":
    unittest.main()
