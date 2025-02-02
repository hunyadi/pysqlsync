import ipaddress
import unittest
from datetime import date, datetime, time, timedelta, timezone

from strong_typing.inspection import DataclassInstance

from pysqlsync.base import GeneratorOptions
from pysqlsync.formation.py_to_sql import ArrayMode, EnumMode
from tests import tables
from tests.params import (
    MSSQLBase,
    MySQLBase,
    OracleBase,
    PostgreSQLBase,
    TestEngineBase,
    configure,
    has_env_var,
)

if __name__ == "__main__":
    configure()


class TestGenerator(TestEngineBase, unittest.TestCase):
    @property
    def options(self) -> GeneratorOptions:
        return GeneratorOptions(namespaces={tables: None})

    def assertMatchSQLCreate(
        self, dialect: str, table: type[DataclassInstance], sql: str
    ) -> None:
        return self.assertMatchSQLCreateOptions(self.options, dialect, table, sql)

    def assertMatchSQLCreateOptions(
        self,
        options: GeneratorOptions,
        dialect: str,
        table: type[DataclassInstance],
        sql: str,
    ) -> None:
        if dialect != self.engine.name:
            return

        statement = self.engine.create_generator(options).create(tables=[table]) or ""
        self.assertMultiLineEqual(statement, sql)

    def assertMatchSQLUpsert(
        self, dialect: str, table: type[DataclassInstance], sql: str
    ) -> None:
        if dialect != self.engine.name:
            return

        generator = self.engine.create_generator(self.options)
        generator.create(tables=[table])
        statement = generator.get_dataclass_upsert_stmt(table)
        self.assertMultiLineEqual(statement, sql)

    def test_create_boolean_table(self) -> None:
        self.maxDiff = None

        self.assertMatchSQLCreate(
            "postgresql",
            tables.BooleanTable,
            'CREATE TABLE "BooleanTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"boolean" boolean NOT NULL,\n'
            '"nullable_boolean" boolean,\n'
            'CONSTRAINT "pk_BooleanTable" PRIMARY KEY ("id")\n'
            ");",
        )
        self.assertMatchSQLCreate(
            "mssql",
            tables.BooleanTable,
            'CREATE TABLE "BooleanTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"boolean" bit NOT NULL,\n'
            '"nullable_boolean" bit,\n'
            'CONSTRAINT "pk_BooleanTable" PRIMARY KEY ("id")\n'
            ");",
        )
        self.assertMatchSQLCreate(
            "mysql",
            tables.BooleanTable,
            'CREATE TABLE "BooleanTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"boolean" tinyint NOT NULL,\n'
            '"nullable_boolean" tinyint,\n'
            'CONSTRAINT "pk_BooleanTable" PRIMARY KEY ("id")\n'
            ");",
        )

    def test_create_numeric_table(self) -> None:
        self.maxDiff = None
        for dialect in ["postgresql", "mssql", "mysql"]:
            with self.subTest(dialect=dialect):
                self.assertMatchSQLCreate(
                    dialect,
                    tables.NumericTable,
                    'CREATE TABLE "NumericTable" (\n'
                    '"id" bigint NOT NULL,\n'
                    '"integer_8" smallint NOT NULL,\n'
                    '"integer_16" smallint NOT NULL,\n'
                    '"integer_32" integer NOT NULL,\n'
                    '"integer_64" bigint NOT NULL,\n'
                    '"integer" bigint NOT NULL,\n'
                    '"nullable_integer" bigint,\n'
                    'CONSTRAINT "pk_NumericTable" PRIMARY KEY ("id")\n'
                    ");",
                )

    def test_create_default_numeric_table(self) -> None:
        self.maxDiff = None
        for dialect in ["postgresql", "mysql"]:
            with self.subTest(dialect=dialect):
                self.assertMatchSQLCreate(
                    dialect,
                    tables.DefaultNumericTable,
                    'CREATE TABLE "DefaultNumericTable" (\n'
                    '"id" bigint NOT NULL,\n'
                    '"integer_8" smallint NOT NULL DEFAULT 127,\n'
                    '"integer_16" smallint NOT NULL DEFAULT 32767,\n'
                    '"integer_32" integer NOT NULL DEFAULT 2147483647,\n'
                    '"integer_64" bigint NOT NULL DEFAULT 0,\n'
                    '"integer" bigint NOT NULL DEFAULT 23,\n'
                    'CONSTRAINT "pk_DefaultNumericTable" PRIMARY KEY ("id")\n'
                    ");",
                )
        self.assertMatchSQLCreate(
            "mssql",
            tables.DefaultNumericTable,
            'CREATE TABLE "DefaultNumericTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"integer_8" smallint NOT NULL CONSTRAINT "df_integer_8" DEFAULT 127,\n'
            '"integer_16" smallint NOT NULL CONSTRAINT "df_integer_16" DEFAULT 32767,\n'
            '"integer_32" integer NOT NULL CONSTRAINT "df_integer_32" DEFAULT 2147483647,\n'
            '"integer_64" bigint NOT NULL CONSTRAINT "df_integer_64" DEFAULT 0,\n'
            '"integer" bigint NOT NULL CONSTRAINT "df_integer" DEFAULT 23,\n'
            'CONSTRAINT "pk_DefaultNumericTable" PRIMARY KEY ("id")\n'
            ");",
        )

    def test_create_fixed_precision_float_table(self) -> None:
        self.maxDiff = None
        for dialect in ["postgresql", "mssql", "mysql"]:
            with self.subTest(dialect=dialect):
                self.assertMatchSQLCreate(
                    dialect,
                    tables.FixedPrecisionFloatTable,
                    'CREATE TABLE "FixedPrecisionFloatTable" (\n'
                    '"id" bigint NOT NULL,\n'
                    '"float_32" real NOT NULL,\n'
                    '"float_64" double precision NOT NULL,\n'
                    '"optional_float_32" real,\n'
                    '"optional_float_64" double precision,\n'
                    'CONSTRAINT "pk_FixedPrecisionFloatTable" PRIMARY KEY ("id")\n'
                    ");",
                )

    def test_create_decimal_table(self) -> None:
        self.maxDiff = None
        for dialect in ["postgresql", "mssql", "mysql"]:
            with self.subTest(dialect=dialect):
                self.assertMatchSQLCreate(
                    dialect,
                    tables.DecimalTable,
                    'CREATE TABLE "DecimalTable" (\n'
                    '"id" bigint NOT NULL,\n'
                    '"decimal_value" decimal NOT NULL,\n'
                    '"optional_decimal" decimal,\n'
                    '"decimal_precision" decimal(5, 2) NOT NULL,\n'
                    'CONSTRAINT "pk_DecimalTable" PRIMARY KEY ("id")\n'
                    ");",
                )

    def test_create_string_table(self) -> None:
        self.maxDiff = None
        self.assertMatchSQLCreate(
            "postgresql",
            tables.StringTable,
            'CREATE TABLE "StringTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"arbitrary_length_string" text NOT NULL,\n'
            '"nullable_arbitrary_length_string" text,\n'
            '"maximum_length_string" varchar(128) NOT NULL,\n'
            '"nullable_maximum_length_string" varchar(128),\n'
            'CONSTRAINT "pk_StringTable" PRIMARY KEY ("id")\n'
            ");",
        )
        self.assertMatchSQLCreate(
            "mssql",
            tables.StringTable,
            'CREATE TABLE "StringTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"arbitrary_length_string" varchar(max) NOT NULL,\n'
            '"nullable_arbitrary_length_string" varchar(max),\n'
            '"maximum_length_string" varchar(128) NOT NULL,\n'
            '"nullable_maximum_length_string" varchar(128),\n'
            'CONSTRAINT "pk_StringTable" PRIMARY KEY ("id")\n'
            ");",
        )
        self.assertMatchSQLCreate(
            "mysql",
            tables.StringTable,
            'CREATE TABLE "StringTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"arbitrary_length_string" mediumtext NOT NULL,\n'
            '"nullable_arbitrary_length_string" mediumtext,\n'
            '"maximum_length_string" varchar(128) NOT NULL,\n'
            '"nullable_maximum_length_string" varchar(128),\n'
            'CONSTRAINT "pk_StringTable" PRIMARY KEY ("id")\n'
            ");",
        )

    def test_create_date_time_table(self) -> None:
        self.maxDiff = None
        self.assertMatchSQLCreate(
            "postgresql",
            tables.DateTimeTable,
            'CREATE TABLE "DateTimeTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"iso_date_time" timestamp NOT NULL,\n'
            '"iso_date" date NOT NULL,\n'
            '"iso_time" time NOT NULL,\n'
            '"optional_date_time" timestamp,\n'
            '"timestamp_precision" timestamp(6) NOT NULL,\n'
            'CONSTRAINT "pk_DateTimeTable" PRIMARY KEY ("id")\n'
            ");",
        )
        self.assertMatchSQLCreate(
            "mssql",
            tables.DateTimeTable,
            'CREATE TABLE "DateTimeTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"iso_date_time" datetime2 NOT NULL,\n'
            '"iso_date" date NOT NULL,\n'
            '"iso_time" time NOT NULL,\n'
            '"optional_date_time" datetime2,\n'
            '"timestamp_precision" datetime2 NOT NULL,\n'
            'CONSTRAINT "pk_DateTimeTable" PRIMARY KEY ("id")\n'
            ");",
        )
        self.assertMatchSQLCreate(
            "mysql",
            tables.DateTimeTable,
            'CREATE TABLE "DateTimeTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"iso_date_time" datetime NOT NULL,\n'
            '"iso_date" date NOT NULL,\n'
            '"iso_time" time NOT NULL,\n'
            '"optional_date_time" datetime,\n'
            '"timestamp_precision" datetime(6) NOT NULL,\n'
            'CONSTRAINT "pk_DateTimeTable" PRIMARY KEY ("id")\n'
            ");",
        )

    def test_create_default_datetime_table(self) -> None:
        self.assertMatchSQLCreate(
            "postgresql",
            tables.DefaultDateTimeTable,
            'CREATE TABLE "DefaultDateTimeTable" (\n'
            '"id" bigint NOT NULL,\n'
            """"iso_date_time" timestamp NOT NULL DEFAULT '1989-10-24 23:59:59',\n"""
            'CONSTRAINT "pk_DefaultDateTimeTable" PRIMARY KEY ("id")\n'
            ");",
        )
        self.assertMatchSQLCreate(
            "oracle",
            tables.DefaultDateTimeTable,
            'CREATE TABLE "DefaultDateTimeTable" (\n'
            '"id" number NOT NULL,\n'
            """"iso_date_time" timestamp DEFAULT TIMESTAMP '1989-10-24 23:59:59' NOT NULL,\n"""
            'CONSTRAINT "pk_DefaultDateTimeTable" PRIMARY KEY ("id")\n'
            ");",
        )
        self.assertMatchSQLCreate(
            "mysql",
            tables.DefaultDateTimeTable,
            'CREATE TABLE "DefaultDateTimeTable" (\n'
            '"id" bigint NOT NULL,\n'
            """"iso_date_time" datetime NOT NULL DEFAULT '1989-10-24 23:59:59',\n"""
            'CONSTRAINT "pk_DefaultDateTimeTable" PRIMARY KEY ("id")\n'
            ");",
        )
        self.assertMatchSQLCreate(
            "mssql",
            tables.DefaultDateTimeTable,
            'CREATE TABLE "DefaultDateTimeTable" (\n'
            '"id" bigint NOT NULL,\n'
            """"iso_date_time" datetime2 NOT NULL CONSTRAINT "df_iso_date_time" DEFAULT '1989-10-24 23:59:59',\n"""
            'CONSTRAINT "pk_DefaultDateTimeTable" PRIMARY KEY ("id")\n'
            ");",
        )

    def test_create_enum_table(self) -> None:
        self.maxDiff = None
        options = GeneratorOptions(initialize_tables=True, namespaces={tables: None})
        self.assertMatchSQLCreate(
            "postgresql",
            tables.EnumTable,
            """CREATE TYPE "WorkflowState" AS ENUM ('active', 'inactive', 'deleted');\n"""
            'CREATE TABLE "EnumTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"state" "WorkflowState" NOT NULL,\n'
            '"optional_state" "WorkflowState",\n'
            'CONSTRAINT "pk_EnumTable" PRIMARY KEY ("id")\n'
            ");",
        )
        self.assertMatchSQLCreate(
            "mysql",
            tables.EnumTable,
            'CREATE TABLE "EnumTable" (\n'
            '"id" bigint NOT NULL,\n'
            """"state" ENUM ('active', 'inactive', 'deleted') CHARACTER SET ascii COLLATE ascii_bin NOT NULL,\n"""
            """"optional_state" ENUM ('active', 'inactive', 'deleted') CHARACTER SET ascii COLLATE ascii_bin,\n"""
            'CONSTRAINT "pk_EnumTable" PRIMARY KEY ("id")\n'
            ");",
        )
        self.assertMatchSQLCreateOptions(
            options,
            "mssql",
            tables.EnumTable,
            'CREATE TABLE "EnumTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"state" integer NOT NULL,\n'
            '"optional_state" integer,\n'
            'CONSTRAINT "pk_EnumTable" PRIMARY KEY ("id")\n'
            ");\n"
            'CREATE TABLE "WorkflowState" (\n'
            '"id" integer NOT NULL IDENTITY,\n'
            '"value" varchar(64) NOT NULL,\n'
            'CONSTRAINT "pk_WorkflowState" PRIMARY KEY ("id")\n'
            ");\n"
            """INSERT INTO "WorkflowState" ("value") VALUES ('active'), ('inactive'), ('deleted');\n"""
            'ALTER TABLE "EnumTable" ADD\n'
            'CONSTRAINT "fk_EnumTable_state" FOREIGN KEY ("state") REFERENCES "WorkflowState" ("id"),\n'
            'CONSTRAINT "fk_EnumTable_optional_state" FOREIGN KEY ("optional_state") REFERENCES "WorkflowState" ("id");\n'
            'ALTER TABLE "WorkflowState" ADD\n'
            'CONSTRAINT "uq_WorkflowState" UNIQUE ("value");',
        )

    def test_create_enum_array_table(self) -> None:
        self.maxDiff = None
        self.assertMatchSQLCreate(
            "postgresql",
            tables.EnumArrayTable,
            """CREATE TYPE "WorkflowState" AS ENUM ('active', 'inactive', 'deleted');\n"""
            'CREATE TABLE "EnumArrayTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"states" "WorkflowState" ARRAY NOT NULL,\n'
            'CONSTRAINT "pk_EnumArrayTable" PRIMARY KEY ("id")\n'
            ");",
        )
        options = GeneratorOptions(
            enum_mode=EnumMode.RELATION,
            array_mode=ArrayMode.RELATION,
            namespaces={tables: None},
        )
        self.assertMatchSQLCreateOptions(
            options,
            "mysql",
            tables.EnumArrayTable,
            'CREATE TABLE "EnumArrayTable" (\n'
            '"id" bigint NOT NULL,\n'
            'CONSTRAINT "pk_EnumArrayTable" PRIMARY KEY ("id")\n'
            ");\n"
            'CREATE TABLE "WorkflowState" (\n'
            '"id" integer NOT NULL AUTO_INCREMENT,\n'
            '"value" varchar(64) NOT NULL,\n'
            'CONSTRAINT "pk_WorkflowState" PRIMARY KEY ("id")\n'
            ");\n"
            'CREATE TABLE "EnumArrayTable_states_WorkflowState" (\n'
            '"uuid" binary(16) NOT NULL,\n'
            '"EnumArrayTable_states" bigint NOT NULL,\n'
            '"WorkflowState_id" integer NOT NULL,\n'
            'CONSTRAINT "pk_EnumArrayTable_states_WorkflowState" PRIMARY KEY ("uuid")\n'
            ");\n"
            'ALTER TABLE "WorkflowState"\n'
            'ADD CONSTRAINT "uq_WorkflowState" UNIQUE ("value");\n'
            'ALTER TABLE "EnumArrayTable_states_WorkflowState"\n'
            'ADD CONSTRAINT "jk_EnumArrayTable_states" FOREIGN KEY ("EnumArrayTable_states") REFERENCES "EnumArrayTable" ("id"),\n'
            'ADD CONSTRAINT "jk_EnumArrayTable_states_WorkflowState" FOREIGN KEY ("WorkflowState_id") REFERENCES "WorkflowState" ("id");',
        )

    def test_create_enum_set_table(self) -> None:
        self.maxDiff = None
        self.assertMatchSQLCreate(
            "postgresql",
            tables.EnumSetTable,
            """CREATE TYPE "WorkflowState" AS ENUM ('active', 'inactive', 'deleted');\n"""
            'CREATE TABLE "EnumSetTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"states" "WorkflowState" ARRAY NOT NULL,\n'
            'CONSTRAINT "pk_EnumSetTable" PRIMARY KEY ("id")\n'
            ");",
        )
        options = GeneratorOptions(
            enum_mode=EnumMode.RELATION,
            array_mode=ArrayMode.RELATION,
            namespaces={tables: None},
        )
        self.assertMatchSQLCreateOptions(
            options,
            "mysql",
            tables.EnumSetTable,
            'CREATE TABLE "EnumSetTable" (\n'
            '"id" bigint NOT NULL,\n'
            'CONSTRAINT "pk_EnumSetTable" PRIMARY KEY ("id")\n'
            ");\n"
            'CREATE TABLE "WorkflowState" (\n'
            '"id" integer NOT NULL AUTO_INCREMENT,\n'
            '"value" varchar(64) NOT NULL,\n'
            'CONSTRAINT "pk_WorkflowState" PRIMARY KEY ("id")\n'
            ");\n"
            'CREATE TABLE "EnumSetTable_states_WorkflowState" (\n'
            '"uuid" binary(16) NOT NULL,\n'
            '"EnumSetTable_states" bigint NOT NULL,\n'
            '"WorkflowState_id" integer NOT NULL,\n'
            'CONSTRAINT "pk_EnumSetTable_states_WorkflowState" PRIMARY KEY ("uuid")\n'
            ");\n"
            'ALTER TABLE "WorkflowState"\n'
            'ADD CONSTRAINT "uq_WorkflowState" UNIQUE ("value");\n'
            'ALTER TABLE "EnumSetTable_states_WorkflowState"\n'
            'ADD CONSTRAINT "jk_EnumSetTable_states" FOREIGN KEY ("EnumSetTable_states") REFERENCES "EnumSetTable" ("id"),\n'
            'ADD CONSTRAINT "jk_EnumSetTable_states_WorkflowState" FOREIGN KEY ("WorkflowState_id") REFERENCES "WorkflowState" ("id"),\n'
            'ADD CONSTRAINT "uq_EnumSetTable_states_WorkflowState" UNIQUE ("WorkflowState_id");',
        )

    def test_create_extensible_enum_table(self) -> None:
        self.maxDiff = None
        options = GeneratorOptions(initialize_tables=True, namespaces={tables: None})
        self.assertMatchSQLCreateOptions(
            options,
            "postgresql",
            tables.ExtensibleEnumTable,
            'CREATE TABLE "ExtensibleEnumTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"state" integer NOT NULL,\n'
            '"optional_state" integer,\n'
            'CONSTRAINT "pk_ExtensibleEnumTable" PRIMARY KEY ("id")\n'
            ");\n"
            'CREATE TABLE "ExtensibleEnum" (\n'
            '"id" integer GENERATED BY DEFAULT AS IDENTITY,\n'
            '"value" varchar(64) NOT NULL,\n'
            'CONSTRAINT "pk_ExtensibleEnum" PRIMARY KEY ("id")\n'
            ");\n"
            """INSERT INTO "ExtensibleEnum" ("value") VALUES ('__unspecified__');\n"""
            'ALTER TABLE "ExtensibleEnumTable"\n'
            'ADD CONSTRAINT "fk_ExtensibleEnumTable_state" FOREIGN KEY ("state") REFERENCES "ExtensibleEnum" ("id"),\n'
            'ADD CONSTRAINT "fk_ExtensibleEnumTable_optional_state" FOREIGN KEY ("optional_state") REFERENCES "ExtensibleEnum" ("id");\n'
            'ALTER TABLE "ExtensibleEnum"\n'
            'ADD CONSTRAINT "uq_ExtensibleEnum" UNIQUE ("value");',
        )

    def test_create_ipaddress_table(self) -> None:
        self.maxDiff = None
        self.assertMatchSQLCreate(
            "postgresql",
            tables.IPAddressTable,
            'CREATE TABLE "IPAddressTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"ipv4" inet NOT NULL,\n'
            '"ipv6" inet NOT NULL,\n'
            '"ipv4_or_ipv6" inet NOT NULL,\n'
            '"optional_ipv4" inet,\n'
            '"optional_ipv6" inet,\n'
            'CONSTRAINT "pk_IPAddressTable" PRIMARY KEY ("id")\n'
            ");",
        )
        for dialect in ["mssql", "mysql"]:
            with self.subTest(dialect=dialect):
                self.assertMatchSQLCreate(
                    dialect,
                    tables.IPAddressTable,
                    'CREATE TABLE "IPAddressTable" (\n'
                    '"id" bigint NOT NULL,\n'
                    '"ipv4" binary(4) NOT NULL,\n'
                    '"ipv6" binary(16) NOT NULL,\n'
                    '"ipv4_or_ipv6" binary(16) NOT NULL,\n'
                    '"optional_ipv4" binary(4),\n'
                    '"optional_ipv6" binary(16),\n'
                    'CONSTRAINT "pk_IPAddressTable" PRIMARY KEY ("id")\n'
                    ");",
                )

    def test_create_literal_table(self) -> None:
        self.maxDiff = None
        self.assertMatchSQLCreate(
            "postgresql",
            tables.LiteralTable,
            'CREATE TABLE "LiteralTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"single" char(5) NOT NULL,\n'
            '"multiple" char(4) NOT NULL,\n'
            '"union" varchar(255) NOT NULL,\n'
            '"unbounded" text NOT NULL,\n'
            'CONSTRAINT "pk_LiteralTable" PRIMARY KEY ("id")\n'
            ");",
        )
        self.assertMatchSQLCreate(
            "mssql",
            tables.LiteralTable,
            'CREATE TABLE "LiteralTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"single" char(5) NOT NULL,\n'
            '"multiple" char(4) NOT NULL,\n'
            '"union" varchar(255) NOT NULL,\n'
            '"unbounded" varchar(max) NOT NULL,\n'
            'CONSTRAINT "pk_LiteralTable" PRIMARY KEY ("id")\n'
            ");",
        )
        self.assertMatchSQLCreate(
            "mysql",
            tables.LiteralTable,
            'CREATE TABLE "LiteralTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"single" char(5) NOT NULL,\n'
            '"multiple" char(4) NOT NULL,\n'
            '"union" varchar(255) NOT NULL,\n'
            '"unbounded" mediumtext NOT NULL,\n'
            'CONSTRAINT "pk_LiteralTable" PRIMARY KEY ("id")\n'
            ");",
        )

    def test_create_primary_key_table(self) -> None:
        self.maxDiff = None
        self.assertMatchSQLCreate(
            "postgresql",
            tables.DataTable,
            'CREATE TABLE "DataTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"data" text NOT NULL,\n'
            'CONSTRAINT "pk_DataTable" PRIMARY KEY ("id")\n'
            ");",
        )
        self.assertMatchSQLCreate(
            "mssql",
            tables.DataTable,
            'CREATE TABLE "DataTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"data" varchar(max) NOT NULL,\n'
            'CONSTRAINT "pk_DataTable" PRIMARY KEY ("id")\n'
            ");",
        )
        self.assertMatchSQLCreate(
            "mysql",
            tables.DataTable,
            'CREATE TABLE "DataTable" (\n'
            '"id" bigint NOT NULL,\n'
            '"data" mediumtext NOT NULL,\n'
            'CONSTRAINT "pk_DataTable" PRIMARY KEY ("id")\n'
            ");",
        )

    def test_create_table_with_description(self) -> None:
        self.maxDiff = None
        self.assertMatchSQLCreate(
            "postgresql",
            tables.Person,
            'CREATE TABLE "Address" (\n'
            '"id" bigint NOT NULL,\n'
            '"city" text NOT NULL,\n'
            '"state" text,\n'
            'CONSTRAINT "pk_Address" PRIMARY KEY ("id")\n'
            ");\n"
            'CREATE TABLE "Person" (\n'
            '"id" bigint NOT NULL,\n'
            '"name" text NOT NULL,\n'
            '"address" bigint NOT NULL,\n'
            'CONSTRAINT "pk_Person" PRIMARY KEY ("id")\n'
            ");\n"
            """COMMENT ON TABLE "Person" IS 'A person.';\n"""
            """COMMENT ON COLUMN "Person"."name" IS 'The person''s full name.';\n"""
            """COMMENT ON COLUMN "Person"."address" IS 'The address of the person''s permanent residence.';\n"""
            'ALTER TABLE "Person"\n'
            'ADD CONSTRAINT "fk_Person_address" FOREIGN KEY ("address") REFERENCES "Address" ("id");',
        )
        self.assertMatchSQLCreate(
            "mssql",
            tables.Person,
            'CREATE TABLE "Address" (\n'
            '"id" bigint NOT NULL,\n'
            '"city" varchar(max) NOT NULL,\n'
            '"state" varchar(max),\n'
            'CONSTRAINT "pk_Address" PRIMARY KEY ("id")\n'
            ");\n"
            'CREATE TABLE "Person" (\n'
            '"id" bigint NOT NULL,\n'
            '"name" varchar(max) NOT NULL,\n'
            """"address" bigint NOT NULL,\n"""
            'CONSTRAINT "pk_Person" PRIMARY KEY ("id")\n'
            ");\n"
            'ALTER TABLE "Person" ADD\n'
            'CONSTRAINT "fk_Person_address" FOREIGN KEY ("address") REFERENCES "Address" ("id");',
        )
        self.assertMatchSQLCreate(
            "mysql",
            tables.Person,
            'CREATE TABLE "Address" (\n'
            '"id" bigint NOT NULL,\n'
            '"city" mediumtext NOT NULL,\n'
            '"state" mediumtext,\n'
            'CONSTRAINT "pk_Address" PRIMARY KEY ("id")\n'
            ");\n"
            'CREATE TABLE "Person" (\n'
            '"id" bigint NOT NULL,\n'
            """"name" mediumtext NOT NULL COMMENT 'The person''s full name.',\n"""
            """"address" bigint NOT NULL COMMENT 'The address of the person''s permanent residence.',\n"""
            'CONSTRAINT "pk_Person" PRIMARY KEY ("id")\n'
            ")\n"
            "COMMENT = 'A person.';\n"
            'ALTER TABLE "Person"\n'
            'ADD CONSTRAINT "fk_Person_address" FOREIGN KEY ("address") REFERENCES "Address" ("id");',
        )

    def test_create_type_with_description(self) -> None:
        self.maxDiff = None
        self.assertMatchSQLCreate(
            "postgresql",
            tables.Location,
            'CREATE TYPE "Coordinates" AS (\n'
            '"lat" double precision,\n'
            '"long" double precision\n'
            ");\n"
            """COMMENT ON TYPE "Coordinates" IS 'Coordinates in the geographic coordinate system.';\n"""
            """COMMENT ON COLUMN "Coordinates"."lat" IS 'Latitude in degrees.';\n"""
            """COMMENT ON COLUMN "Coordinates"."long" IS 'Longitude in degrees.';\n"""
            'CREATE TABLE "Location" (\n'
            '"id" bigint NOT NULL,\n'
            '"coords" "Coordinates" NOT NULL,\n'
            'CONSTRAINT "pk_Location" PRIMARY KEY ("id")\n'
            ");",
        )

    def test_insert_single(self) -> None:
        self.maxDiff = None
        self.assertMatchSQLUpsert(
            "postgresql",
            tables.DataTable,
            'INSERT INTO "DataTable"\n'
            '("id", "data") VALUES ($1, $2)\n'
            'ON CONFLICT ("id") DO UPDATE SET\n'
            '"data" = EXCLUDED."data"\n'
            ";",
        )

        for dialect in ["mssql", "oracle"]:
            with self.subTest(dialect=dialect):
                generator = self.engine.create_generator(self.options)
                value_list = f"({generator.placeholder(1)}, {generator.placeholder(2)})"
                self.assertMatchSQLUpsert(
                    dialect,
                    tables.DataTable,
                    'MERGE INTO "DataTable" target\n'
                    f'USING (VALUES {value_list}) source("id", "data")\n'
                    'ON (target."id" = source."id")\n'
                    "WHEN MATCHED THEN\n"
                    'UPDATE SET target."data" = source."data"\n'
                    "WHEN NOT MATCHED THEN\n"
                    'INSERT ("id", "data") VALUES (source."id", source."data")\n'
                    ";",
                )

        self.assertMatchSQLUpsert(
            "mysql",
            tables.DataTable,
            'INSERT INTO "DataTable"\n'
            '("id", "data") VALUES (%s, %s)\n'
            "ON DUPLICATE KEY UPDATE\n"
            '"data" = VALUES("data")\n'
            ";",
        )

    def test_insert_multiple(self) -> None:
        self.maxDiff = None
        self.assertMatchSQLUpsert(
            "postgresql",
            tables.BooleanTable,
            'INSERT INTO "BooleanTable"\n'
            '("id", "boolean", "nullable_boolean") VALUES ($1, $2, $3)\n'
            'ON CONFLICT ("id") DO UPDATE SET\n'
            '"boolean" = EXCLUDED."boolean",\n'
            '"nullable_boolean" = EXCLUDED."nullable_boolean"\n'
            ";",
        )

        for dialect in ["mssql", "oracle"]:
            with self.subTest(dialect=dialect):
                generator = self.engine.create_generator(self.options)
                value_list = f"({generator.placeholder(1)}, {generator.placeholder(2)}, {generator.placeholder(3)})"
                self.assertMatchSQLUpsert(
                    dialect,
                    tables.BooleanTable,
                    'MERGE INTO "BooleanTable" target\n'
                    f'USING (VALUES {value_list}) source("id", "boolean", "nullable_boolean")\n'
                    'ON (target."id" = source."id")\n'
                    "WHEN MATCHED THEN\n"
                    "UPDATE SET "
                    'target."boolean" = source."boolean", '
                    'target."nullable_boolean" = source."nullable_boolean"\n'
                    "WHEN NOT MATCHED THEN\n"
                    'INSERT ("id", "boolean", "nullable_boolean") VALUES (source."id", source."boolean", source."nullable_boolean")\n'
                    ";",
                )

        self.assertMatchSQLUpsert(
            "mysql",
            tables.BooleanTable,
            'INSERT INTO "BooleanTable"\n'
            '("id", "boolean", "nullable_boolean") VALUES (%s, %s, %s)\n'
            "ON DUPLICATE KEY UPDATE\n"
            '"boolean" = VALUES("boolean"),\n'
            '"nullable_boolean" = VALUES("nullable_boolean")\n'
            ";",
        )

    def test_table_data(self) -> None:
        generator = self.engine.create_generator(self.options)
        generator.create(
            tables=[
                tables.DataTable,
                tables.StringTable,
            ]
        )
        self.assertEqual(
            generator.get_dataclass_as_record(
                tables.DataTable, tables.DataTable(123, "abc")
            ),
            (123, "abc"),
        )
        self.assertEqual(
            generator.get_dataclass_as_record(
                tables.StringTable, tables.StringTable(1, "abc", None, "def", None)
            ),
            (1, "abc", None, "def", None),
        )
        self.assertEqual(
            generator.get_dataclass_as_record(
                tables.StringTable, tables.StringTable(2, "abc", "def", "ghi", "jkl")
            ),
            (2, "abc", "def", "ghi", "jkl"),
        )

    def test_table_data_datetime(self) -> None:
        generator = self.engine.create_generator(self.options)
        generator.create(tables=[tables.DateTimeTable])
        self.assertEqual(
            generator.get_dataclass_as_record(
                tables.DateTimeTable,
                tables.DateTimeTable(
                    1,
                    datetime(1982, 10, 23, 23, 59, 59, tzinfo=timezone.utc),
                    date(2023, 1, 1),
                    time(23, 59, 59, tzinfo=timezone.utc),
                    None,
                    datetime(1984, 1, 1, 23, 59, 59, tzinfo=timezone.utc),
                ),
            ),
            (
                1,
                datetime(1982, 10, 23, 23, 59, 59, tzinfo=timezone.utc),
                date(2023, 1, 1),
                time(23, 59, 59, tzinfo=timezone.utc),
                None,
                datetime(1984, 1, 1, 23, 59, 59, tzinfo=timezone.utc),
            ),
        )

    def test_table_data_ipaddress(self) -> None:
        generator = self.engine.create_generator(self.options)
        generator.create(tables=[tables.IPAddressTable])
        self.assertEqual(
            generator.get_dataclass_as_record(
                tables.IPAddressTable,
                tables.IPAddressTable(
                    1,
                    ipaddress.IPv4Address("192.168.0.1"),
                    ipaddress.IPv6Address("2001:db8::"),
                    ipaddress.IPv6Address("2001:db8::"),
                    None,
                    None,
                ),
            ),
            (
                1,
                b"\xc0\xa8\x00\x01",
                b" \x01\r\xb8\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
                b" \x01\r\xb8\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
                None,
                None,
            ),
        )


@unittest.skipUnless(has_env_var("ORACLE"), "Oracle tests are disabled")
class TestOracleGenerator(OracleBase, TestGenerator):
    def test_table_data_datetime(self) -> None:
        generator = self.engine.create_generator(self.options)
        generator.create(tables=[tables.DateTimeTable])
        self.assertEqual(
            generator.get_dataclass_as_record(
                tables.DateTimeTable,
                tables.DateTimeTable(
                    1,
                    datetime(1982, 10, 23, 23, 59, 59, tzinfo=timezone.utc),
                    date(2023, 1, 1),
                    time(23, 59, 59, tzinfo=timezone.utc),
                    None,
                    datetime(1984, 1, 1, 23, 59, 59, tzinfo=timezone.utc),
                ),
            ),
            (
                1,
                datetime(1982, 10, 23, 23, 59, 59, tzinfo=timezone.utc),
                date(2023, 1, 1),
                timedelta(seconds=86399),
                None,
                datetime(1984, 1, 1, 23, 59, 59, tzinfo=timezone.utc),
            ),
        )


@unittest.skipUnless(has_env_var("POSTGRESQL"), "PostgreSQL tests are disabled")
class TestPostgreSQLGenerator(PostgreSQLBase, TestGenerator):
    def test_table_data_enum(self) -> None:
        generator = self.engine.create_generator(self.options)
        generator.create(tables=[tables.EnumTable])
        self.assertEqual(
            generator.get_dataclass_as_record(
                tables.EnumTable, tables.EnumTable(1, tables.WorkflowState.active, None)
            ),
            (1, "active", None),
        )

    def test_table_data_ipaddress(self) -> None:
        generator = self.engine.create_generator(self.options)
        generator.create(tables=[tables.IPAddressTable])
        self.assertEqual(
            generator.get_dataclass_as_record(
                tables.IPAddressTable,
                tables.IPAddressTable(
                    1,
                    ipaddress.IPv4Address("192.168.0.1"),
                    ipaddress.IPv6Address("2001:db8::"),
                    ipaddress.IPv6Address("2001:db8::"),
                    None,
                    None,
                ),
            ),
            (
                1,
                ipaddress.IPv4Address("192.168.0.1"),
                ipaddress.IPv6Address("2001:db8::"),
                ipaddress.IPv6Address("2001:db8::"),
                None,
                None,
            ),
        )


@unittest.skipUnless(has_env_var("MSSQL"), "Microsoft SQL tests are disabled")
class TestMSSQLGenerator(MSSQLBase, TestGenerator):
    pass


@unittest.skipUnless(has_env_var("MYSQL"), "MySQL tests are disabled")
class TestMySQLGenerator(MySQLBase, TestGenerator):
    pass


del TestGenerator

if __name__ == "__main__":
    unittest.main()
