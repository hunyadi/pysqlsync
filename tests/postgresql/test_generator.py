import enum
import unittest
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Optional

from pysqlsync.base import PrimaryKey
from pysqlsync.factory import get_engine
from strong_typing.auxiliary import (
    Annotated,
    MaxLength,
    Precision,
    TimePrecision,
    float32,
    float64,
    int8,
    int16,
    int32,
    int64,
    uint8,
    uint16,
    uint32,
    uint64,
)


class WorkflowState(enum.Enum):
    active = "active"
    inactive = "inactive"
    deleted = "deleted"


@dataclass
class NumericTable:
    boolean: bool
    nullable_boolean: Optional[bool]

    signed_integer_8: int8
    signed_integer_16: int16
    signed_integer_32: int32
    signed_integer_64: int64
    unsigned_integer_8: uint8
    unsigned_integer_16: uint16
    unsigned_integer_32: uint32
    unsigned_integer_64: uint64

    float_32: float32
    float_64: float64

    integer: int
    nullable_integer: Optional[int]


@dataclass
class StringTable:
    arbitrary_length_string: str
    nullable_arbitrary_length_string: Optional[str]
    maximum_length_string: Annotated[str, MaxLength(255)]
    nullable_maximum_length_string: Optional[Annotated[str, MaxLength(255)]]


@dataclass
class DateTimeTable:
    iso_date_time: datetime
    iso_date: date
    iso_time: time
    optional_date_time: Optional[datetime]


@dataclass
class EnumTable:
    state: WorkflowState


@dataclass
class ExampleTable:
    id: PrimaryKey[int]
    data: str


Generator = get_engine("postgresql").get_generator_type()


def get_create_table(table: type) -> str:
    generator = Generator(table)
    return generator.get_create_table_stmt().rstrip()


def get_insert(table: type) -> str:
    generator = Generator(table)
    return generator.get_insert_stmt().rstrip()


class TestGenerator(unittest.TestCase):
    def test_create_numeric_table(self):
        lines = [
            'CREATE TABLE "NumericTable" (',
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
            '"iso_date_time" timestamp without time zone NOT NULL,',
            '"iso_date" date NOT NULL,',
            '"iso_time" time without time zone NOT NULL,',
            '"optional_date_time" timestamp without time zone',
            ")",
        ]
        self.assertMultiLineEqual(get_create_table(DateTimeTable), "\n".join(lines))

    def test_create_primary_key_table(self):
        lines = [
            'CREATE TABLE "ExampleTable" (',
            '"id" integer PRIMARY KEY,',
            '"data" text NOT NULL',
            ")",
        ]
        self.assertMultiLineEqual(get_create_table(ExampleTable), "\n".join(lines))

    def test_insert(self):
        lines = [
            'INSERT INTO "ExampleTable"',
            '("id", "data") VALUES ($1, $2)',
            'ON CONFLICT("id") DO UPDATE SET',
            '"id" = EXCLUDED."id",',
            '"data" = EXCLUDED."data"',
        ]
        self.assertMultiLineEqual(get_insert(ExampleTable), "\n".join(lines))


if __name__ == "__main__":
    unittest.main()
