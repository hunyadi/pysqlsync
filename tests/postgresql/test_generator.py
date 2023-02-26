import unittest
from datetime import timezone

from pysqlsync.factory import get_engine
from tests.tables import *

Generator = get_engine("postgresql").get_generator_type()


def get_create_table(table: type) -> str:
    generator = Generator(table)
    return generator.get_create_table_stmt().rstrip()


def get_insert(table: type) -> str:
    generator = Generator(table)
    return generator.get_upsert_stmt().rstrip()


class TestGenerator(unittest.TestCase):
    def test_create_numeric_table(self):
        lines = [
            'CREATE TABLE "NumericTable" (',
            '"id" integer PRIMARY KEY,',
            '"boolean" boolean NOT NULL,',
            '"nullable_boolean" boolean,',
            '"signed_integer_8" int1 NOT NULL,',
            '"signed_integer_16" smallint NOT NULL,',
            '"signed_integer_32" int NOT NULL,',
            '"signed_integer_64" bigint NOT NULL,',
            '"unsigned_integer_8" uint1 NOT NULL,',
            '"unsigned_integer_16" uint2 NOT NULL,',
            '"unsigned_integer_32" uint4 NOT NULL,',
            '"unsigned_integer_64" uint8 NOT NULL,',
            '"float_32" real NOT NULL,',
            '"float_64" double precision NOT NULL,',
            '"integer" integer NOT NULL,',
            '"nullable_integer" integer',
            ")",
        ]
        self.assertMultiLineEqual(get_create_table(NumericTable), "\n".join(lines))

    def test_create_string_table(self):
        lines = [
            'CREATE TABLE "StringTable" (',
            '"id" integer PRIMARY KEY,',
            '"arbitrary_length_string" text NOT NULL,',
            '"nullable_arbitrary_length_string" text,',
            '"maximum_length_string" character varying(255) NOT NULL,',
            '"nullable_maximum_length_string" character varying(255)',
            ")",
        ]
        self.assertMultiLineEqual(get_create_table(StringTable), "\n".join(lines))

    def test_create_date_time_table(self):
        lines = [
            'CREATE TABLE "DateTimeTable" (',
            '"id" integer PRIMARY KEY,',
            '"iso_date_time" timestamp without time zone NOT NULL,',
            '"iso_date" date NOT NULL,',
            '"iso_time" time without time zone NOT NULL,',
            '"optional_date_time" timestamp without time zone',
            ")",
        ]
        self.assertMultiLineEqual(get_create_table(DateTimeTable), "\n".join(lines))

    def test_create_enum_table(self):
        lines = [
            'CREATE TABLE "EnumTable" (',
            '"id" integer PRIMARY KEY,',
            "\"state\" text NOT NULL CHECK (\"state\" IN ('active', 'inactive', 'deleted'))",
            ")",
        ]
        self.assertMultiLineEqual(get_create_table(EnumTable), "\n".join(lines))

    def test_create_primary_key_table(self):
        lines = [
            'CREATE TABLE "DataTable" (',
            '"id" integer PRIMARY KEY,',
            '"data" text NOT NULL',
            ")",
        ]
        self.assertMultiLineEqual(get_create_table(DataTable), "\n".join(lines))

    def test_insert(self):
        lines = [
            'INSERT INTO "DataTable"',
            '("id", "data") VALUES ($1, $2)',
            'ON CONFLICT("id") DO UPDATE SET',
            '"data" = EXCLUDED."data"',
        ]
        self.assertMultiLineEqual(get_insert(DataTable), "\n".join(lines))

        lines = [
            'INSERT INTO "DateTimeTable"',
            '("id", "iso_date_time", "iso_date", "iso_time", "optional_date_time") VALUES ($1, $2, $3, $4, $5)',
            'ON CONFLICT("id") DO UPDATE SET',
            '"iso_date_time" = EXCLUDED."iso_date_time",',
            '"iso_date" = EXCLUDED."iso_date",',
            '"iso_time" = EXCLUDED."iso_time",',
            '"optional_date_time" = EXCLUDED."optional_date_time"',
        ]
        self.assertMultiLineEqual(get_insert(DateTimeTable), "\n".join(lines))

    def test_table_data(self):
        generator = Generator(DataTable)
        self.assertEqual(
            generator.get_record_as_tuple(DataTable(123, "abc")), (123, "abc")
        )

        generator = Generator(StringTable)
        self.assertEqual(
            generator.get_record_as_tuple(StringTable(1, "abc", None, "def", None)),
            (1, "abc", None, "def", None),
        )
        self.assertEqual(
            generator.get_record_as_tuple(StringTable(2, "abc", "def", "ghi", "jkl")),
            (2, "abc", "def", "ghi", "jkl"),
        )

        generator = Generator(DateTimeTable)
        self.assertEqual(
            generator.get_record_as_tuple(
                DateTimeTable(
                    1,
                    datetime(1982, 10, 23, 23, 59, 59, tzinfo=timezone.utc),
                    date(2023, 1, 1),
                    time(23, 59, 59, tzinfo=timezone.utc),
                    None,
                )
            ),
            (
                1,
                datetime(1982, 10, 23, 23, 59, 59, tzinfo=timezone.utc),
                date(2023, 1, 1),
                time(23, 59, 59, tzinfo=timezone.utc),
                None,
            ),
        )

        generator = Generator(EnumTable)
        self.assertEqual(
            generator.get_record_as_tuple(EnumTable(1, WorkflowState.active)),
            (1, "active"),
        )


if __name__ == "__main__":
    unittest.main()
