import unittest
import uuid
from datetime import datetime

import tests.tables
from pysqlsync.base import ConnectionParameters, GeneratorOptions
from pysqlsync.factory import get_engine
from tests.tables import DataTable, UserTable, WorkflowState
from tests.timed_test import TimedAsyncioTestCase

engine = get_engine("postgresql")


class TestConnection(TimedAsyncioTestCase):
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
        return GeneratorOptions(namespaces={tests.tables: None})

    async def test_connection(self) -> None:
        async with engine.create_connection(self.parameters, self.options) as conn:
            await conn.drop_table(DataTable, ignore_missing=True)
            await conn.execute(
                'CREATE TABLE "DataTable" ("id" int PRIMARY KEY, "data" text)'
            )

    async def test_insert(self) -> None:
        async with engine.create_connection(self.parameters, self.options) as conn:
            await conn.drop_table(DataTable, ignore_missing=True)

            generator = engine.create_generator(DataTable, self.options)
            statement = generator.get_create_stmt()
            await conn.execute(statement)
            statement = generator.get_upsert_stmt()
            records = generator.get_records_as_tuples(
                [DataTable(1, "a"), DataTable(2, "b"), DataTable(3, "c")]
            )
            await conn.execute_all(statement, records)

    async def test_bulk_insert(self) -> None:
        async with engine.create_connection(self.parameters, self.options) as conn:
            await conn.drop_table(DataTable, ignore_missing=True)

            generator = engine.create_generator(
                DataTable, GeneratorOptions(namespaces={tests.tables: None})
            )
            statement = generator.get_create_stmt()
            await conn.execute(statement)
            statement = generator.get_upsert_stmt()
            for i in range(10):
                records = generator.get_records_as_tuples(
                    [DataTable(i * 1000 + j, str(i * 1000 + j)) for j in range(1000)]
                )
                await conn.execute_all(statement, records)

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

        async with engine.create_connection(self.parameters, self.options) as conn:
            await conn.drop_table(UserTable, ignore_missing=True)
            await conn.create_table(UserTable)
            await conn.insert_data(UserTable, data)

    async def test_dataclass_upsert(self) -> None:
        data = self.get_user_data()

        async with engine.create_connection(self.parameters, self.options) as conn:
            await conn.drop_table(UserTable, ignore_missing=True)
            await conn.create_table(UserTable)
            await conn.upsert_data(UserTable, data)


if __name__ == "__main__":
    unittest.main()
