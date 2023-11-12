import datetime
import ipaddress
import uuid
from typing import Any, Callable, Optional

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
from pysqlsync.model.data_types import SqlFixedBinaryType
from pysqlsync.util.typing import override

from .data_types import MSSQLBooleanType, MSSQLDateTimeType, MSSQLVariableCharacterType
from .mutation import MSSQLMutator
from .object_types import MSSQLObjectFactory


class MSSQLGenerator(BaseGenerator):
    """
    Generator for Microsoft SQL Server (T-SQL).

    Assumes a UTF-8 collation and `SET ANSI_DEFAULTS ON`. UTF-8 collation makes `varchar` store UTF-8 characters.
    """

    def __init__(self, options: GeneratorOptions) -> None:
        super().__init__(options, MSSQLObjectFactory(), MSSQLMutator())

        if options.enum_mode is EnumMode.TYPE or options.enum_mode is EnumMode.INLINE:
            raise FormationError(
                f"unsupported enum conversion mode for {self.__class__.__name__}: {options.enum_mode}"
            )
        if options.struct_mode is StructMode.TYPE:
            raise FormationError(
                f"unsupported struct conversion mode for {self.__class__.__name__}: {options.struct_mode}"
            )
        if options.array_mode is ArrayMode.ARRAY:
            raise FormationError(
                f"unsupported array conversion mode for {self.__class__.__name__}: {options.array_mode}"
            )

        self.converter = DataclassConverter(
            options=DataclassConverterOptions(
                enum_mode=options.enum_mode or EnumMode.RELATION,
                struct_mode=options.struct_mode or StructMode.JSON,
                array_mode=options.array_mode or ArrayMode.JSON,
                namespaces=NamespaceMapping(options.namespaces),
                foreign_constraints=options.foreign_constraints,
                substitutions={
                    bool: MSSQLBooleanType(),
                    datetime.datetime: MSSQLDateTimeType(),
                    str: MSSQLVariableCharacterType(),
                    uuid.UUID: SqlFixedBinaryType(16),
                    JsonType: MSSQLVariableCharacterType(),
                    ipaddress.IPv4Address: SqlFixedBinaryType(4),
                    ipaddress.IPv6Address: SqlFixedBinaryType(16),
                },
                factory=self.factory,
                skip_annotations=options.skip_annotations,
            )
        )

    @override
    def get_table_insert_stmt(self, table: Table) -> str:
        statements: list[str] = []
        statements.append(f"INSERT INTO {table.name}")
        columns = [column for column in table.columns.values() if not column.identity]
        column_list = ", ".join(str(column.name) for column in columns)
        value_list = ", ".join("?" for _ in columns)
        statements.append(f"({column_list}) VALUES ({value_list})")
        return "\n".join(statements)

    def _get_merge_preamble(self, table: Table, columns: list[Column]) -> list[str]:
        statements: list[str] = []

        statements.append(f"MERGE INTO {table.name} AS target")
        column_list = ", ".join(str(column.name) for column in columns)
        value_list = ", ".join("?" for _ in columns)
        statements.append(f"USING (VALUES ({value_list})) AS source({column_list})")

        match_columns = [
            column
            for column in table.columns.values()
            if not column.identity and table.is_lookup_column(column.name)
        ]
        match_condition = " OR ".join(
            f"target.{column.name} = source.{column.name}" for column in match_columns
        )
        statements.append(f"ON {match_condition}")

        return statements

    @override
    def get_table_merge_stmt(self, table: Table) -> str:
        columns = [column for column in table.columns.values() if not column.identity]
        statements = self._get_merge_preamble(table, columns)

        statements.append("WHEN NOT MATCHED BY TARGET THEN")
        column_list = ", ".join(str(column.name) for column in columns)
        insert_list = ", ".join(f"source.{column.name}" for column in columns)
        statements.append(f"INSERT ({column_list}) VALUES ({insert_list})")

        statements.append(";")
        return "\n".join(statements)

    @override
    def get_table_upsert_stmt(self, table: Table) -> str:
        columns = [column for column in table.columns.values() if not column.identity]
        statements: list[str] = self._get_merge_preamble(table, columns)

        statements.append("WHEN MATCHED THEN")
        update_list = ", ".join(
            f"target.{column.name} = source.{column.name}" for column in columns
        )
        statements.append(f"UPDATE SET {update_list}")

        statements.append("WHEN NOT MATCHED BY TARGET THEN")
        column_list = ", ".join(str(column.name) for column in columns)
        insert_list = ", ".join(f"source.{column.name}" for column in columns)
        statements.append(f"INSERT ({column_list}) VALUES ({insert_list})")

        statements.append(";")
        return "\n".join(statements)

    @override
    def get_field_extractor(
        self, column: Column, field_name: str, field_type: type
    ) -> Callable[[Any], Any]:
        if field_type is uuid.UUID:
            return lambda obj: getattr(obj, field_name).bytes
        elif field_type is ipaddress.IPv4Address or field_type is ipaddress.IPv6Address:
            return lambda obj: getattr(obj, field_name).packed

        return super().get_field_extractor(column, field_name, field_type)

    @override
    def get_value_transformer(
        self, column: Column, field_type: type
    ) -> Optional[Callable[[Any], Any]]:
        if field_type is uuid.UUID:
            return lambda field: field.bytes
        elif field_type is ipaddress.IPv4Address or field_type is ipaddress.IPv6Address:
            return lambda field: field.packed

        return super().get_value_transformer(column, field_type)
