import dataclasses
import datetime
import os.path
import random
import unittest
import uuid

from params import PostgreSQLBase
from tsv.helper import Generator, Parser

from pysqlsync.base import GeneratorOptions
from pysqlsync.formation.py_to_sql import (
    DataclassConverter,
    DataclassConverterOptions,
    EnumMode,
    NamespaceMapping,
)
from pysqlsync.model.id_types import QualifiedId
from pysqlsync.model.properties import get_field_properties
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


class TestPostgreSQLConnection(unittest.IsolatedAsyncioTestCase, PostgreSQLBase):
    @property
    def options(self) -> GeneratorOptions:
        return GeneratorOptions(namespaces={tables: "performance_test"})

    async def test_rows_upsert(self) -> None:
        async with self.engine.create_connection(self.parameters, self.options) as conn:
            options = DataclassConverterOptions(
                enum_mode=EnumMode.RELATION,
                namespaces=NamespaceMapping({tables: "performance_test"}),
                column_class=conn.connection.generator.column_class,
            )
            converter = DataclassConverter(options=options)
            catalog = converter.dataclasses_to_catalog([tables.EventRecord])

            await conn.execute('DROP SCHEMA IF EXISTS "performance_test" CASCADE')
            await conn.execute(str(catalog))
            conn.connection.generator.catalog = catalog

            column_types = tuple(
                get_field_properties(field.type).tsv_type
                for field in dataclasses.fields(tables.EventRecord)
            )
            parser = Parser(column_types)

            table = catalog.get_table(
                QualifiedId("performance_test", tables.EventRecord.__name__)
            )

            with Timer("read file"):
                with open(
                    os.path.join(os.path.dirname(__file__), "performance_test.tsv"),
                    "rb",
                ) as f:
                    data = parser.parse_file(f)

            with Timer("insert into database table"):
                await conn.upsert_rows(table, column_types, data)

            self.assertEqual(
                await conn.query_one(
                    int, 'SELECT COUNT(*) FROM "performance_test"."EventRecord";'
                ),
                999999,
            )
            # await conn.execute('DROP SCHEMA "performance_test" CASCADE')


if __name__ == "__main__":
    unittest.main()
