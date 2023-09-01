import typing
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
    def test_create_numeric_table(self) -> None:
        lines = [
            'CREATE TABLE "NumericTable" (',
            '"id" bigint NOT NULL,',
            '"boolean" boolean NOT NULL,',
            '"nullable_boolean" boolean,',
            '"signed_integer_8" smallint NOT NULL,',
            '"signed_integer_16" smallint NOT NULL,',
            '"signed_integer_32" integer NOT NULL,',
            '"signed_integer_64" bigint NOT NULL,',
            '"unsigned_integer_8" smallint NOT NULL,',
            '"unsigned_integer_16" smallint NOT NULL,',
            '"unsigned_integer_32" integer NOT NULL,',
            '"unsigned_integer_64" bigint NOT NULL,',
            '"float_32" real NOT NULL,',
            '"float_64" double precision NOT NULL,',
            '"integer" bigint NOT NULL,',
            '"nullable_integer" bigint,',
            'PRIMARY KEY ("id")',
            ");",
        ]
        self.assertMultiLineEqual(get_create_table(NumericTable), "\n".join(lines))

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
        self.assertMultiLineEqual(get_create_table(StringTable), "\n".join(lines))

    def test_create_date_time_table(self) -> None:
        lines = [
            'CREATE TABLE "DateTimeTable" (',
            '"id" bigint NOT NULL,',
            '"iso_date_time" timestamp without time zone NOT NULL,',
            '"iso_date" date NOT NULL,',
            '"iso_time" time without time zone NOT NULL,',
            '"optional_date_time" timestamp without time zone,',
            'PRIMARY KEY ("id")',
            ");",
        ]
        self.assertMultiLineEqual(get_create_table(DateTimeTable), "\n".join(lines))

    def test_create_enum_table(self) -> None:
        lines = [
            'CREATE TABLE "EnumTable" (',
            '"id" bigint NOT NULL,',
            '"state" text NOT NULL,',
            'PRIMARY KEY ("id"),',
            """CONSTRAINT "ch_EnumTable_state" CHECK ("state" IN ('active', 'inactive', 'deleted'))""",
            ");",
        ]
        self.assertMultiLineEqual(get_create_table(EnumTable), "\n".join(lines))

    def test_create_ipaddress_table(self) -> None:
        lines = [
            'CREATE TABLE "IPAddressTable" (',
            '"id" bigint NOT NULL,',
            '"ipv4" inet NOT NULL,',
            '"ipv6" inet NOT NULL,',
            'PRIMARY KEY ("id")',
            ");",
        ]
        self.assertMultiLineEqual(get_create_table(IPAddressTable), "\n".join(lines))

    def test_create_primary_key_table(self) -> None:
        lines = [
            'CREATE TABLE "DataTable" (',
            '"id" bigint NOT NULL,',
            '"data" text NOT NULL,',
            'PRIMARY KEY ("id")',
            ");",
        ]
        self.assertMultiLineEqual(get_create_table(DataTable), "\n".join(lines))

    def test_insert(self) -> None:
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

    def test_table_data(self) -> None:
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

        generator = Generator(IPAddressTable)
        self.assertEqual(
            generator.get_record_as_tuple(
                IPAddressTable(
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
