import ipaddress
import typing
import unittest
from datetime import date, datetime, time, timezone

from strong_typing.inspection import DataclassInstance

import tests.tables as tables
from pysqlsync.base import BaseGenerator, GeneratorOptions
from pysqlsync.factory import get_dialect


def get_generator() -> BaseGenerator:
    return get_dialect("postgresql").create_generator(
        GeneratorOptions(namespaces={tables: None})
    )


def get_create_stmt(table: type[DataclassInstance]) -> str:
    statement = get_generator().create(tables=[table])
    return statement or ""


class TestGenerator(unittest.TestCase):
    def test_create_boolean_table(self) -> None:
        lines = [
            'CREATE TABLE "BooleanTable" (',
            '"id" bigint NOT NULL,',
            '"boolean" boolean NOT NULL,',
            '"nullable_boolean" boolean,',
            'PRIMARY KEY ("id")',
            ");",
        ]
        self.assertMultiLineEqual(
            get_create_stmt(tables.BooleanTable), "\n".join(lines)
        )

    def test_create_numeric_table(self) -> None:
        lines = [
            'CREATE TABLE "NumericTable" (',
            '"id" bigint NOT NULL,',
            '"signed_integer_8" smallint NOT NULL,',
            '"signed_integer_16" smallint NOT NULL,',
            '"signed_integer_32" integer NOT NULL,',
            '"signed_integer_64" bigint NOT NULL,',
            '"unsigned_integer_8" smallint NOT NULL,',
            '"unsigned_integer_16" smallint NOT NULL,',
            '"unsigned_integer_32" integer NOT NULL,',
            '"unsigned_integer_64" bigint NOT NULL,',
            '"integer" bigint NOT NULL,',
            '"nullable_integer" bigint,',
            'PRIMARY KEY ("id")',
            ");",
        ]
        self.assertMultiLineEqual(
            get_create_stmt(tables.NumericTable), "\n".join(lines)
        )

    def test_create_float_table(self) -> None:
        lines = [
            'CREATE TABLE "FloatTable" (',
            '"id" bigint NOT NULL,',
            '"float_32" real NOT NULL,',
            '"float_64" double precision NOT NULL,',
            'PRIMARY KEY ("id")',
            ");",
        ]
        self.assertMultiLineEqual(get_create_stmt(tables.FloatTable), "\n".join(lines))

    def test_create_string_table(self) -> None:
        lines = [
            'CREATE TABLE "StringTable" (',
            '"id" bigint NOT NULL,',
            '"arbitrary_length_string" text NOT NULL,',
            '"nullable_arbitrary_length_string" text,',
            '"maximum_length_string" varchar(255) NOT NULL,',
            '"nullable_maximum_length_string" varchar(255),',
            'PRIMARY KEY ("id")',
            ");",
        ]
        self.assertMultiLineEqual(get_create_stmt(tables.StringTable), "\n".join(lines))

    def test_create_date_time_table(self) -> None:
        lines = [
            'CREATE TABLE "DateTimeTable" (',
            '"id" bigint NOT NULL,',
            '"iso_date_time" timestamp NOT NULL,',
            '"iso_date" date NOT NULL,',
            '"iso_time" time NOT NULL,',
            '"optional_date_time" timestamp,',
            'PRIMARY KEY ("id")',
            ");",
        ]
        self.assertMultiLineEqual(
            get_create_stmt(tables.DateTimeTable), "\n".join(lines)
        )

    def test_create_enum_table(self) -> None:
        lines = [
            """CREATE TYPE "WorkflowState" AS ENUM ('active', 'inactive', 'deleted');""",
            'CREATE TABLE "EnumTable" (',
            '"id" bigint NOT NULL,',
            '"state" "WorkflowState" NOT NULL,',
            'PRIMARY KEY ("id")',
            ");",
        ]
        self.assertMultiLineEqual(get_create_stmt(tables.EnumTable), "\n".join(lines))

    def test_create_ipaddress_table(self) -> None:
        lines = [
            'CREATE TABLE "IPAddressTable" (',
            '"id" bigint NOT NULL,',
            '"ipv4" inet NOT NULL,',
            '"ipv6" inet NOT NULL,',
            'PRIMARY KEY ("id")',
            ");",
        ]
        self.assertMultiLineEqual(
            get_create_stmt(tables.IPAddressTable), "\n".join(lines)
        )

    def test_create_primary_key_table(self) -> None:
        lines = [
            'CREATE TABLE "DataTable" (',
            '"id" bigint NOT NULL,',
            '"data" text NOT NULL,',
            'PRIMARY KEY ("id")',
            ");",
        ]
        self.assertMultiLineEqual(get_create_stmt(tables.DataTable), "\n".join(lines))

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
            'ALTER TABLE "Person"',
            'ADD CONSTRAINT "fk_Person_address" FOREIGN KEY ("address") REFERENCES "Address" ("id")',
            ";",
            """COMMENT ON TABLE "Person" IS 'A person.';""",
            """COMMENT ON COLUMN "Person"."address" IS 'The address of the person''s permanent residence.';""",
        ]
        self.assertMultiLineEqual(get_create_stmt(tables.Person), "\n".join(lines))

    def test_create_type_with_description(self) -> None:
        lines = [
            'CREATE TYPE "Coordinates" AS (',
            '"lat" double precision,',
            '"long" double precision',
            ");",
            'CREATE TABLE "Location" (',
            '"id" bigint NOT NULL,',
            '"coords" "Coordinates" NOT NULL,',
            'PRIMARY KEY ("id")',
            ");",
            """COMMENT ON TYPE "Coordinates" IS 'Coordinates in the geographic coordinate system.';""",
            """COMMENT ON COLUMN "Coordinates"."lat" IS 'Latitude in degrees.';""",
            """COMMENT ON COLUMN "Coordinates"."long" IS 'Longitude in degrees.';""",
        ]
        self.assertMultiLineEqual(get_create_stmt(tables.Location), "\n".join(lines))

    def test_insert_single(self) -> None:
        generator = get_generator()
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
        generator = get_generator()
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
        generator = get_generator()

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
