import unittest
from dataclasses import dataclass

from pysqlsync.base import Parameters, PrimaryKey
from pysqlsync.factory import get_engine

engine = get_engine("postgresql")
Generator = engine.get_generator_type()
Connection = engine.get_connection_type()


@dataclass
class DataTable:
    id: PrimaryKey[int]
    data: str


class TestConnection(unittest.IsolatedAsyncioTestCase):
    params: Parameters

    def parameters(self) -> Parameters:
        return Parameters(
            host="localhost",
            port=5432,
            username="postgres",
            password=None,
            database="levente.hunyadi",
        )

    async def test_connection(self):
        async with Connection(self.parameters()) as conn:
            await conn.execute('DROP TABLE IF EXISTS "DataTable"')
            await conn.execute(
                'CREATE TABLE "DataTable" ("id" int PRIMARY KEY, "data" text)'
            )

    async def test_insert(self):
        async with Connection(self.parameters()) as conn:
            await conn.execute('DROP TABLE IF EXISTS "DataTable"')

            generator = Generator(DataTable)
            statement = generator.get_create_table_stmt()
            await conn.execute(statement)
            statement = generator.get_insert_stmt()
            await conn.execute_all(statement, [(1, "a"), (2, "b"), (3, "c")])

    async def test_bulk_insert(self):
        async with Connection(self.parameters()) as conn:
            await conn.execute('DROP TABLE IF EXISTS "DataTable"')

            generator = Generator(DataTable)
            statement = generator.get_create_table_stmt()
            await conn.execute(statement)
            statement = generator.get_insert_stmt()
            await conn.execute_all(
                statement, [(index, str(index)) for index in range(1000000)]
            )


if __name__ == "__main__":
    unittest.main()
