import datetime
import ipaddress
import uuid
from dataclasses import dataclass

from strong_typing.core import JsonType

from pysqlsync.base import BaseGenerator, GeneratorOptions
from pysqlsync.formation.object_types import Column, FormationError, Table
from pysqlsync.formation.py_to_sql import (
    ArrayMode,
    DataclassConverter,
    DataclassConverterOptions,
    EnumMode,
    NamespaceMapping,
    StructMode,
)
from pysqlsync.model.data_types import SqlCharacterType, SqlFixedBinaryType, constant

from .data_types import MSSQLBooleanType, MSSQLCharacterType, MSSQLDateTimeType


@dataclass
class MSSQLColumn(Column):
    @property
    def data_spec(self) -> str:
        nullable = " NOT NULL" if not self.nullable else ""
        default = (
            f" DEFAULT {constant(self.default)}" if self.default is not None else ""
        )
        identity = " IDENTITY" if self.identity else ""
        return f"{self.data_type}{nullable}{default}{identity}"

    def mutate_column_stmt(target: "MSSQLColumn", source: Column) -> list[str]:
        if source.identity != target.identity:
            raise FormationError(
                "operation not permitted; cannot add or drop identity property"
            )
        return super().mutate_column_stmt(source)


class MSSQLGenerator(BaseGenerator):
    """
    Generator for Microsoft T-SQL.

    Assumes a UTF-8 collation and `SET ANSI_DEFAULTS ON`. UTF-8 collation makes `varchar` store UTF-8 characters.
    """

    def __init__(self, options: GeneratorOptions) -> None:
        super().__init__(options)

        if options.enum_mode is EnumMode.TYPE:
            raise FormationError(
                f"unsupported enum conversion mode for {self.__class__.__name__}: {options.enum_mode}"
            )
        if options.struct_mode is StructMode.TYPE:
            raise FormationError(
                f"unsupported struct conversion mode for {self.__class__.__name__}: {options.struct_mode}"
            )

        self.converter = DataclassConverter(
            options=DataclassConverterOptions(
                enum_mode=options.enum_mode or EnumMode.RELATION,
                struct_mode=options.struct_mode or StructMode.JSON,
                array_mode=ArrayMode.JSON,
                namespaces=NamespaceMapping(options.namespaces),
                foreign_constraints=options.foreign_constraints,
                substitutions={
                    bool: MSSQLBooleanType(),
                    datetime.datetime: MSSQLDateTimeType(),
                    str: MSSQLCharacterType(),
                    uuid.UUID: SqlFixedBinaryType(16),
                    JsonType: SqlCharacterType(),
                    ipaddress.IPv4Address: SqlFixedBinaryType(4),
                    ipaddress.IPv6Address: SqlFixedBinaryType(16),
                },
                skip_annotations=options.skip_annotations,
                column_class=MSSQLColumn,
            )
        )

    @property
    def column_class(self) -> type[Column]:
        return MSSQLColumn

    def get_table_insert_stmt(self, table: Table) -> str:
        statements: list[str] = []
        statements.append(f"INSERT INTO {table.name}")
        columns = [column for column in table.columns.values() if not column.identity]
        column_list = ", ".join(str(column.name) for column in columns)
        value_list = ", ".join("?" for _ in columns)
        statements.append(f"({column_list}) VALUES ({value_list})")
        return "\n".join(statements)

    def get_table_upsert_stmt(self, table: Table) -> str:
        statements: list[str] = []

        statements.append(f"MERGE INTO {table.name} AS target")
        columns = [column for column in table.columns.values() if not column.identity]
        column_list = ", ".join(str(column.name) for column in columns)
        value_list = ", ".join("?" for _ in columns)
        statements.append(f"USING (VALUES ({value_list})) AS source({column_list})")
        statements.append(f"ON target.{table.primary_key} = source.{table.primary_key}")

        statements.append("WHEN MATCHED THEN")
        update_list = ", ".join(f"target.{c.name} = source.{c.name}" for c in columns)
        statements.append(f"UPDATE SET {update_list}")

        statements.append("WHEN NOT MATCHED BY TARGET THEN")
        insert_list = ", ".join(f"source.{column.name}" for column in columns)
        statements.append(f"INSERT ({column_list}) VALUES ({insert_list})")

        statements.append(";")
        return "\n".join(statements)
