import copy
import unittest
from dataclasses import dataclass
from io import BytesIO

from strong_typing.inspection import dataclass_fields

import tests.tables as tables
from pysqlsync.base import GeneratorOptions
from pysqlsync.data.exchange import TextReader, TextWriter
from pysqlsync.data.generator import random_objects
from pysqlsync.formation.inspection import get_entity_types
from pysqlsync.formation.object_types import FormationError
from pysqlsync.formation.py_to_sql import ArrayMode, EnumMode, StructMode
from pysqlsync.model.id_types import LocalId
from tests.params import MSSQLBase, MySQLBase, PostgreSQLBase, TestEngineBase


@dataclass
class TestOptions:
    enum_mode: EnumMode
    array_mode: ArrayMode
    struct_mode: StructMode


class TestSynchronize(TestEngineBase, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.drop_schema(LocalId("sample"))

    @property
    def options(self) -> GeneratorOptions:
        return GeneratorOptions(
            namespaces={tables: "sample"}, foreign_constraints=False
        )

    async def test_create_new(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            explorer = self.engine.create_explorer(conn)
            self.assertFalse(conn.connection.generator.state.namespaces)
            await explorer.synchronize(module=tables)
            self.assertTrue(conn.connection.generator.state.namespaces)

    async def test_update_existing(self) -> None:
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

    async def test_insert_data(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            explorer = self.engine.create_explorer(conn)
            await explorer.synchronize(module=tables)

        async with self.engine.create_connection(self.parameters, self.options) as conn:
            explorer = self.engine.create_explorer(conn)
            await explorer.synchronize(module=tables)

            entity_types = get_entity_types([tables])
            for entity_type in entity_types:
                entities = random_objects(entity_type, 100)

                # generate random data and write records
                with BytesIO() as stream:
                    writer = TextWriter(stream, entity_type)
                    writer.write_objects(entities)
                    data = stream.getvalue()

                # read and parse data into records
                with BytesIO(data) as stream:
                    reader = TextReader(stream, entity_type)
                    columns, field_types = reader.columns, reader.field_types
                    rows = reader.read_records()

                # insert/update data in database table
                table = conn.get_table(entity_type)
                await conn.insert_rows(
                    table,
                    field_names=columns,
                    field_types=field_types,
                    records=rows,
                )


class TestPostgreSQLSynchronize(PostgreSQLBase, TestSynchronize):
    pass


class TestMSSQLSynchronize(MSSQLBase, TestSynchronize):
    pass


class TestMySQLSynchronize(MySQLBase, TestSynchronize):
    pass


del TestSynchronize

if __name__ == "__main__":
    unittest.main()
