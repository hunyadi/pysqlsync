import dataclasses
import datetime
import os.path
import random
import unittest
import uuid

from params import MySQLBase, PostgreSQLBase, TestEngineBase
from tsv.helper import Generator, Parser

from pysqlsync.base import GeneratorOptions
from pysqlsync.formation.py_to_sql import EnumMode
from pysqlsync.model.properties import get_class_properties
from tests import tables
from tests.datagen import random_alphanumeric_str, random_datetime, random_enum
from tests.measure import Timer


def create_randomized_object(index: int) -> tables.EventRecord:
    return tables.EventRecord(
        id=index,
        guid=uuid.uuid4(),
        timestamp=random_datetime(
            datetime.datetime(1982, 10, 23, 2, 30, 0, tzinfo=datetime.timezone.utc),
            datetime.datetime(2022, 9, 25, 18, 45, 0, tzinfo=datetime.timezone.utc),
        ),
        user_id=random.randint(1, 1000000),
        real_user_id=random.randint(1, 1000000),
        # expires_on=random_date(datetime.date(1982, 10, 23), datetime.date(2022, 9, 25)),
        interaction_duration=random.uniform(0.0, 1.0),
        url="https://example.com/" + random_alphanumeric_str(1, 128),
        user_agent=f"Mozilla/5.0 (platform; rv:0.{random.randint(0, 9)}) Gecko/{random.randint(1, 99)} Firefox/{random.randint(1, 99)}",
        http_method=random_enum(tables.HTTPMethod),
        http_status=random_enum(tables.HTTPStatus),
        http_version=random_enum(tables.HTTPVersion),
        # remote_ip=random_ipv4address(),
        participated=random.uniform(0.0, 1.0) > 0.5,
    )


def generate_input_file() -> None:
    "Generates data and writes a file to be used as input for performance testing."

    generator = Generator()
    with open(
        os.path.join(os.path.dirname(__file__), "performance_test.tsv"), "wb"
    ) as f:
        for k in range(1, 1000000):
            f.write(
                generator.generate_line(
                    dataclasses.astuple(create_randomized_object(k))
                )
            )
            f.write(b"\n")


class TestPerformance(TestEngineBase, unittest.IsolatedAsyncioTestCase):
    @property
    def options(self) -> GeneratorOptions:
        return GeneratorOptions(
            enum_mode=EnumMode.RELATION, namespaces={tables: "performance_test"}
        )

    async def test_rows_upsert(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.create_objects([tables.EventRecord])

            table = conn.get_table(tables.EventRecord)
            column_types = get_class_properties(tables.EventRecord).tsv_types
            parser = Parser(column_types)

            with Timer("read file"):
                with open(
                    os.path.join(os.path.dirname(__file__), "performance_test.tsv"),
                    "rb",
                ) as f:
                    data = parser.parse_file(f)

            with Timer("insert into database table"):
                await conn.upsert_rows(table, column_types, data)

            self.assertEqual(
                await conn.query_one(int, f"SELECT COUNT(*) FROM {table.name};"),
                999,
            )

            await conn.drop_objects()


class TestPostgreSQLConnection(PostgreSQLBase, TestPerformance):
    async def asyncSetUp(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.execute('DROP SCHEMA IF EXISTS "performance_test" CASCADE;')


class TestMySQLConnection(MySQLBase, TestPerformance):
    async def asyncSetUp(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            await conn.execute(
                'DROP TABLE IF EXISTS "performance_test__EventRecord", "performance_test__HTTPMethod", "performance_test__HTTPStatus", "performance_test__HTTPVersion";'
            )


del TestPerformance

if __name__ == "__main__":
    unittest.main()
