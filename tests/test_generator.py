import ipaddress
import typing
import unittest
from datetime import date, datetime, time, timezone

from strong_typing.inspection import DataclassInstance

import tests.tables as tables
from pysqlsync.base import BaseGenerator, GeneratorOptions
from pysqlsync.factory import get_dialect


def get_generator(dialect: str) -> BaseGenerator:
    return get_dialect(dialect).create_generator(
        GeneratorOptions(namespaces={tables: None})
    )


def get_create_stmt(table: type[DataclassInstance], dialect: str) -> str:
    statement = get_generator(dialect=dialect).create(tables=[table])
    return statement or ""


class TestGenerator(unittest.TestCase):
    def test_create_boolean_table(self) -> None:
        for dialect in ["postgresql", "mysql"]:
            with self.subTest(dialect=dialect):
                self.assertMultiLineEqual(
                    get_create_stmt(tables.BooleanTable, dialect=dialect),
                    'CREATE TABLE "BooleanTable" (\n'
                    '"id" bigint NOT NULL,\n'
                    '"boolean" boolean NOT NULL,\n'
                    '"nullable_boolean" boolean,\n'
                    'PRIMARY KEY ("id")\n'
                    ");",
                )

    def test_create_numeric_table(self) -> None:
        for dialect in ["postgresql", "mysql"]:
            with self.subTest(dialect=dialect):
                self.assertMultiLineEqual(
                    get_create_stmt(tables.NumericTable, dialect=dialect),
                    'CREATE TABLE "NumericTable" (\n'
                    '"id" bigint NOT NULL,\n'
                    '"signed_integer_8" smallint NOT NULL,\n'
                    '"signed_integer_16" smallint NOT NULL,\n'
                    '"signed_integer_32" integer NOT NULL,\n'
                    '"signed_integer_64" bigint NOT NULL,\n'
                    '"unsigned_integer_8" smallint NOT NULL,\n'
                    '"unsigned_integer_16" smallint NOT NULL,\n'
                    '"unsigned_integer_32" integer NOT NULL,\n'
                    '"unsigned_integer_64" bigint NOT NULL,\n'
                    '"integer" bigint NOT NULL,\n'
                    '"nullable_integer" bigint,\n'
                    'PRIMARY KEY ("id")\n'
                    ");",
                )

    def test_create_float_table(self) -> None:
        for dialect in ["postgresql", "mysql"]:
            with self.subTest(dialect=dialect):
                self.assertMultiLineEqual(
                    get_create_stmt(tables.FloatTable, dialect=dialect),
                    'CREATE TABLE "FloatTable" (\n'
                    '"id" bigint NOT NULL,\n'
                    '"float_32" real NOT NULL,\n'
                    '"float_64" double precision NOT NULL,\n'
                    'PRIMARY KEY ("id")\n'
                    ");",
                )

    def test_create_string_table(self) -> None:
        for dialect in ["postgresql", "mysql"]:
            with self.subTest(dialect=dialect):
                self.assertMultiLineEqual(
                    get_create_stmt(tables.StringTable, dialect=dialect),
                    'CREATE TABLE "StringTable" (\n'
                    '"id" bigint NOT NULL,\n'
                    '"arbitrary_length_string" text NOT NULL,\n'
                    '"nullable_arbitrary_length_string" text,\n'
                    '"maximum_length_string" varchar(255) NOT NULL,\n'
                    '"nullable_maximum_length_string" varchar(255),\n'
                    'PRIMARY KEY ("id")\n'
                    ");",
                )

    def test_create_date_time_table(self) -> None:
        self.assertMultiLineEqual(
            get_create_stmt(tables.DateTimeTable, dialect="postgresql"),
            'CREATE TABLE "DateTimeTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"iso_date_time" timestamp NOT NULL,\n'
            '"iso_date" date NOT NULL,\n'
            '"iso_time" time NOT NULL,\n'
            '"optional_date_time" timestamp,\n'
            'PRIMARY KEY ("id")\n'
            ");",
        )
        self.assertMultiLineEqual(
            get_create_stmt(tables.DateTimeTable, dialect="mysql"),
            'CREATE TABLE "DateTimeTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"iso_date_time" datetime NOT NULL,\n'
            '"iso_date" date NOT NULL,\n'
            '"iso_time" time NOT NULL,\n'
            '"optional_date_time" datetime,\n'
            'PRIMARY KEY ("id")\n'
            ");",
        )

    def test_create_enum_table(self) -> None:
        self.assertMultiLineEqual(
            get_create_stmt(tables.EnumTable, dialect="postgresql"),
            """CREATE TYPE "WorkflowState" AS ENUM ('active', 'inactive', 'deleted');\n"""
            'CREATE TABLE "EnumTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"state" "WorkflowState" NOT NULL,\n'
            'PRIMARY KEY ("id")\n'
            ");",
        )
        self.assertMultiLineEqual(
            get_create_stmt(tables.EnumTable, dialect="mysql"),
            'CREATE TABLE "EnumTable" (\n'
            '"id" bigint NOT NULL,\n'
            """"state" ENUM ('active', 'inactive', 'deleted') NOT NULL,\n"""
            'PRIMARY KEY ("id")\n'
            ");",
        )

    def test_create_ipaddress_table(self) -> None:
        self.assertMultiLineEqual(
            get_create_stmt(tables.IPAddressTable, dialect="postgresql"),
            'CREATE TABLE "IPAddressTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"ipv4" inet NOT NULL,\n'
            '"ipv6" inet NOT NULL,\n'
            'PRIMARY KEY ("id")\n'
            ");",
        )
        self.assertMultiLineEqual(
            get_create_stmt(tables.IPAddressTable, dialect="mysql"),
            'CREATE TABLE "IPAddressTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"ipv4" binary(4) NOT NULL,\n'
            '"ipv6" binary(16) NOT NULL,\n'
            'PRIMARY KEY ("id")\n'
            ");",
        )

    def test_create_primary_key_table(self) -> None:
        for dialect in ["postgresql", "mysql"]:
            with self.subTest(dialect=dialect):
                self.assertMultiLineEqual(
                    get_create_stmt(tables.DataTable, dialect=dialect),
                    'CREATE TABLE "DataTable" (\n'
                    '"id" bigint NOT NULL,\n'
                    '"data" text NOT NULL,\n'
                    'PRIMARY KEY ("id")\n'
                    ");",
                )

    def test_create_table_with_description(self) -> None:
        lines = [
            'CREATE TABLE "Address" (',
            '"id" bigint NOT NULL,',
            '"city" text NOT NULL,',
            '"state" text,',
            'PRIMARY KEY ("id")',
            ");",
            'CREATE TABLE "Person" (',
            '"id" bigint NOT NULL,',
            '"address" bigint NOT NULL,',
            'PRIMARY KEY ("id")',
            ");",
            """COMMENT ON TABLE "Person" IS 'A person.';""",
            """COMMENT ON COLUMN "Person"."address" IS 'The address of the person''s permanent residence.';""",
            'ALTER TABLE "Person"',
            'ADD CONSTRAINT "fk_Person_address" FOREIGN KEY ("address") REFERENCES "Address" ("id")',
            ";",
        ]
        self.assertMultiLineEqual(
            get_create_stmt(tables.Person, dialect="postgresql"), "\n".join(lines)
        )

    def test_create_type_with_description(self) -> None:
        lines = [
            'CREATE TYPE "Coordinates" AS (',
            '"lat" double precision,',
            '"long" double precision',
            ");",
            """COMMENT ON TYPE "Coordinates" IS 'Coordinates in the geographic coordinate system.';""",
            """COMMENT ON COLUMN "Coordinates"."lat" IS 'Latitude in degrees.';""",
            """COMMENT ON COLUMN "Coordinates"."long" IS 'Longitude in degrees.';""",
            'CREATE TABLE "Location" (',
            '"id" bigint NOT NULL,',
            '"coords" "Coordinates" NOT NULL,',
            'PRIMARY KEY ("id")',
            ");",
        ]
        self.assertMultiLineEqual(
            get_create_stmt(tables.Location, dialect="postgresql"), "\n".join(lines)
        )

    def test_insert_single(self) -> None:
        generator = get_generator(dialect="postgresql")
        generator.create(tables=[tables.DataTable])

        lines = [
            'INSERT INTO "DataTable"',
            '("id", "data") VALUES ($1, $2)',
            'ON CONFLICT ("id") DO UPDATE SET',
            '"data" = EXCLUDED."data"',
        ]
        self.assertMultiLineEqual(
            generator.get_dataclass_upsert_stmt(tables.DataTable), "\n".join(lines)
        )

    def test_insert_multiple(self) -> None:
        generator = get_generator(dialect="postgresql")
        generator.create(tables=[tables.DateTimeTable])

        lines = [
            'INSERT INTO "DateTimeTable"',
            '("id", "iso_date_time", "iso_date", "iso_time", "optional_date_time") VALUES ($1, $2, $3, $4, $5)',
            'ON CONFLICT ("id") DO UPDATE SET',
            '"iso_date_time" = EXCLUDED."iso_date_time",',
            '"iso_date" = EXCLUDED."iso_date",',
            '"iso_time" = EXCLUDED."iso_time",',
            '"optional_date_time" = EXCLUDED."optional_date_time"',
        ]
        self.assertMultiLineEqual(
            generator.get_dataclass_upsert_stmt(tables.DateTimeTable), "\n".join(lines)
        )

    def test_table_data(self) -> None:
        generator = get_generator(dialect="postgresql")

        self.assertEqual(
            generator.get_dataclass_as_record(tables.DataTable(123, "abc")),
            (123, "abc"),
        )
        self.assertEqual(
            generator.get_dataclass_as_record(
                tables.StringTable(1, "abc", None, "def", None)
            ),
            (1, "abc", None, "def", None),
        )
        self.assertEqual(
            generator.get_dataclass_as_record(
                tables.StringTable(2, "abc", "def", "ghi", "jkl")
            ),
            (2, "abc", "def", "ghi", "jkl"),
        )
        self.assertEqual(
            generator.get_dataclass_as_record(
                tables.DateTimeTable(
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
        self.assertEqual(
            generator.get_dataclass_as_record(
                tables.EnumTable(1, tables.WorkflowState.active)
            ),
            (1, "active"),
        )
        self.assertEqual(
            generator.get_dataclass_as_record(
                tables.IPAddressTable(
                    1,
                    typing.cast(
                        ipaddress.IPv4Address, ipaddress.ip_address("192.168.0.1")
                    ),
                    typing.cast(
                        ipaddress.IPv6Address, ipaddress.ip_address("2001:db8::")
                    ),
                )
            ),
            (1, "192.168.0.1", "2001:db8::"),
        )


if __name__ == "__main__":
    unittest.main()
