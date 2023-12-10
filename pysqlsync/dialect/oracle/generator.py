import datetime
import ipaddress
import uuid
from typing import Any, Callable, Optional

from strong_typing.auxiliary import int8, int16, int32, int64
from strong_typing.core import JsonType

from pysqlsync.base import BaseGenerator, GeneratorOptions
from pysqlsync.formation.inspection import is_ip_address_type
from pysqlsync.formation.object_types import Column, FormationError, Table
from pysqlsync.formation.py_to_sql import (
    ArrayMode,
    DataclassConverter,
    DataclassConverterOptions,
    EnumMode,
    NamespaceMapping,
    StructMode,
)
from pysqlsync.util.typing import override

from .data_types import (
    OracleIntegerType,
    OracleTimestampType,
    OracleVariableBinaryType,
    OracleVariableCharacterType,
)
from .object_types import OracleObjectFactory


class OracleGenerator(BaseGenerator):
    "Generator for Oracle."

    converter: DataclassConverter

    def __init__(self, options: GeneratorOptions) -> None:
        super().__init__(options, OracleObjectFactory())

        if options.enum_mode is EnumMode.TYPE:
            raise FormationError(
                f"unsupported enum conversion mode for {self.__class__.__name__}: {options.enum_mode}"
            )

        self.converter = DataclassConverter(
            options=DataclassConverterOptions(
                enum_mode=options.enum_mode or EnumMode.INLINE,
                struct_mode=options.struct_mode or StructMode.JSON,
                array_mode=options.array_mode or ArrayMode.JSON,
                qualified_names=False,
                namespaces=NamespaceMapping(options.namespaces),
                foreign_constraints=options.foreign_constraints,
                substitutions={
                    bytes: OracleVariableBinaryType(),
                    datetime.datetime: OracleTimestampType(),
                    int: OracleIntegerType(),
                    int8: OracleIntegerType(),
                    int16: OracleIntegerType(),
                    int32: OracleIntegerType(),
                    int64: OracleIntegerType(),
                    str: OracleVariableCharacterType(),
                    uuid.UUID: OracleVariableBinaryType(16),
                    JsonType: OracleVariableCharacterType(),
                    ipaddress.IPv4Address: OracleVariableBinaryType(4),
                    ipaddress.IPv6Address: OracleVariableBinaryType(16),
                },
                skip_annotations=options.skip_annotations,
                factory=self.factory,
            )
        )

    @override
    def get_table_insert_stmt(
        self, table: Table, order: Optional[tuple[str, ...]] = None
    ) -> str:
        statements: list[str] = []
        statements.append(f"INSERT INTO {table.name}")
        columns = [column for column in table.get_columns(order) if not column.identity]
        column_list = ", ".join(str(column.name) for column in columns)
        value_list = ", ".join(f":{index}" for index, _ in enumerate(columns, start=1))
        statements.append(f"({column_list}) VALUES ({value_list})")
        statements.append(";")
        return "\n".join(statements)

    def _get_merge_preamble(self, table: Table, columns: list[Column]) -> list[str]:
        statements: list[str] = []

        statements.append(f"MERGE INTO {table.name} target")
        column_list = ", ".join(str(column.name) for column in columns)
        value_list = ", ".join(f":{index}" for index, _ in enumerate(columns, start=1))
        statements.append(f"USING (VALUES ({value_list})) source({column_list})")

        match_columns = [column for column in columns if table.is_lookup_column(column)]
        if match_columns:
            match_condition = " OR ".join(
                f"target.{column.name} = source.{column.name}"
                for column in match_columns
            )

            statements.append(f"ON ({match_condition})")

        return statements

    @override
    def get_table_merge_stmt(
        self, table: Table, order: Optional[tuple[str, ...]] = None
    ) -> str:
        columns = [column for column in table.get_columns(order) if not column.identity]
        statements = self._get_merge_preamble(table, columns)

        statements.append("WHEN NOT MATCHED THEN")
        column_list = ", ".join(str(column.name) for column in columns)
        insert_list = ", ".join(f"source.{column.name}" for column in columns)
        statements.append(f"INSERT ({column_list}) VALUES ({insert_list})")

        statements.append(";")
        return "\n".join(statements)

    @override
    def get_table_upsert_stmt(
        self, table: Table, order: Optional[tuple[str, ...]] = None
    ) -> str:
        columns = [column for column in table.get_columns(order)]
        statements: list[str] = self._get_merge_preamble(table, columns)

        insert_columns = [column for column in columns if not column.identity]
        update_columns = [
            column for column in insert_columns if not table.is_lookup_column(column)
        ]
        if update_columns:
            statements.append("WHEN MATCHED THEN")
            update_list = ", ".join(
                f"target.{column.name} = source.{column.name}"
                for column in update_columns
            )
            statements.append(f"UPDATE SET {update_list}")
        if insert_columns:
            statements.append("WHEN NOT MATCHED THEN")
            column_list = ", ".join(str(column.name) for column in insert_columns)
            insert_list = ", ".join(
                f"source.{column.name}" for column in insert_columns
            )
            statements.append(f"INSERT ({column_list}) VALUES ({insert_list})")

        statements.append(";")
        return "\n".join(statements)

    @override
    def get_table_delete_stmt(self, table: Table) -> str:
        return f"DELETE FROM {table.name} WHERE {table.get_primary_column().name} = :1"

    @override
    def get_field_extractor(
        self, column: Column, field_name: str, field_type: type
    ) -> Callable[[Any], Any]:
        if field_type is uuid.UUID:
            return lambda obj: getattr(obj, field_name).bytes
        elif is_ip_address_type(field_type):
            return lambda obj: getattr(obj, field_name).packed

        return super().get_field_extractor(column, field_name, field_type)

    @override
    def get_value_transformer(
        self, column: Column, field_type: type
    ) -> Optional[Callable[[Any], Any]]:
        if field_type is uuid.UUID:
            return lambda field: field.bytes
        elif is_ip_address_type(field_type):
            return lambda field: field.packed

        return super().get_value_transformer(column, field_type)
