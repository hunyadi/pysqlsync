import uuid
from datetime import datetime

import tests.tables
from pysqlsync.base import GeneratorOptions
from tests.params import TestEngineBase
from tests.tables import DataTable, UserTable, WorkflowState
from tests.timed_test import TimedAsyncioTestCase


class TestConnection(TimedAsyncioTestCase, TestEngineBase):
    @property
    def options(self) -> GeneratorOptions:
        return GeneratorOptions(namespaces={tests.tables: None})

    async def test_connection(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.execute(
                'CREATE TABLE "DataTable" ("id" int PRIMARY KEY, "data" text);'
            )
            await conn.execute('DROP TABLE "DataTable";')

    async def test_insert(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.create_objects([DataTable])
            generator = self.engine.create_generator(self.options)
            statement = generator.get_upsert_stmt(DataTable)
            records = generator.get_dataclasses_as_records(
                [DataTable(1, "a"), DataTable(2, "b"), DataTable(3, "c")]
            )
            await conn.execute_all(statement, records)
            await conn.drop_objects()

    async def test_bulk_insert(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.create_objects([DataTable])
            generator = self.engine.create_generator(self.options)
            statement = generator.get_upsert_stmt(DataTable)
            for i in range(10):
                records = generator.get_dataclasses_as_records(
                    [DataTable(i * 1000 + j, str(i * 1000 + j)) for j in range(1000)]
                )
                await conn.execute_all(statement, records)
            await conn.drop_objects()

    def get_user_data(self) -> list[UserTable]:
        return [
            UserTable(
                id=k,
                created_at=datetime.now(),
                updated_at=datetime.now(),
                deleted_at=datetime.now(),
                workflow_state=WorkflowState.inactive,
                uuid=uuid.uuid4(),
                name="Dr. Levente Hunyadi",
                short_name="Levente",
                sortable_name="Hunyadi, Levente",
                homepage_url="https://hunyadi.info.hu",
            )
            for k in range(10000)
        ]

    async def test_dataclass_insert(self) -> None:
        data = self.get_user_data()

        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.create_objects([UserTable])
            await conn.insert_data(UserTable, data)
            rows = await conn.query_all(int, 'SELECT COUNT(*) FROM "UserTable"')
            self.assertEqual(rows[0], len(data))
            await conn.drop_objects()

    async def test_dataclass_upsert(self) -> None:
        data = self.get_user_data()

        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.create_objects([UserTable])
            await conn.upsert_data(UserTable, data)
            rows = await conn.query_all(int, 'SELECT COUNT(*) FROM "UserTable"')
            self.assertEqual(rows[0], len(data))
            await conn.drop_objects()
