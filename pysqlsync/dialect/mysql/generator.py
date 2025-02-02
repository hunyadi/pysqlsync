import datetime
import ipaddress
import uuid
from typing import Any, Callable, Optional

from pysqlsync.base import BaseGenerator, GeneratorOptions
from pysqlsync.formation.inspection import is_ip_address_type
from pysqlsync.formation.object_types import Column, Table
from pysqlsync.formation.py_to_sql import (
    ArrayMode,
    DataclassConverter,
    DataclassConverterOptions,
    EnumMode,
    NamespaceMapping,
    StructMode,
)
from pysqlsync.model.data_types import SqlFixedBinaryType, SqlIntegerType
from pysqlsync.model.id_types import LocalId
from pysqlsync.util.typing import override

from .data_types import MySQLDateTimeType, MySQLVariableCharacterType
from .mutation import MySQLMutator
from .object_types import MySQLObjectFactory


class MySQLGenerator(BaseGenerator):
    """
    Generator for MySQL.

    Assumes configuration `ANSI_QUOTES` and `SET @@session.time_zone = "+00:00"`.
    """

    def __init__(self, options: GeneratorOptions) -> None:
        super().__init__(
            options, MySQLObjectFactory(), MySQLMutator(options.synchronization)
        )

        self.check_enum_mode(exclude=[EnumMode.TYPE])
        self.check_struct_mode(matches=StructMode.JSON)
        self.check_array_mode(include=[ArrayMode.JSON, ArrayMode.RELATION])

        self.converter = DataclassConverter(
            options=DataclassConverterOptions(
                enum_mode=options.enum_mode or EnumMode.INLINE,
                struct_mode=options.struct_mode or StructMode.JSON,
                array_mode=options.array_mode or ArrayMode.JSON,
                qualified_names=False,
                namespaces=NamespaceMapping(options.namespaces),
                foreign_constraints=options.foreign_constraints,
                initialize_tables=options.initialize_tables,
                substitutions={
                    bool: SqlIntegerType(1),
                    datetime.datetime: MySQLDateTimeType(),
                    uuid.UUID: SqlFixedBinaryType(16),
                    str: MySQLVariableCharacterType(16777215),
                    ipaddress.IPv4Address: SqlFixedBinaryType(4),
                    ipaddress.IPv6Address: SqlFixedBinaryType(16),
                },
                factory=self.factory,
                skip_annotations=options.skip_annotations,
            )
        )

    @override
    def get_current_schema_stmt(self) -> str:
        return "DATABASE()"

    @override
    def placeholder(self, index: int) -> str:
        return r"%s"

    @override
    def get_table_insert_stmt(
        self, table: Table, order: Optional[tuple[str, ...]] = None
    ) -> str:
        statements: list[str] = []
        statements.append(f"INSERT INTO {table.name}")
        columns = [column for column in table.get_columns(order) if not column.identity]
        column_list = ", ".join(str(column.name) for column in columns)
        value_list = ", ".join("%s" for column in columns)
        statements.append(f"({column_list}) VALUES ({value_list})")
        statements.append(";")
        return "\n".join(statements)

    @override
    def get_table_merge_stmt(
        self, table: Table, order: Optional[tuple[str, ...]] = None
    ) -> str:
        statements: list[str] = []
        statements.append(f"INSERT INTO {table.name}")
        columns = [column for column in table.get_columns(order) if not column.identity]
        column_list = ", ".join(str(column.name) for column in columns)
        value_list = ", ".join("%s" for column in columns)
        statements.append(f"({column_list}) VALUES ({value_list})")
        statements.append("ON DUPLICATE KEY UPDATE")
        defs = [f"{key} = {key}" for key in table.primary_key]
        statements.append(",\n".join(defs))
        statements.append(";")
        return "\n".join(statements)

    @override
    def get_table_upsert_stmt(
        self, table: Table, order: Optional[tuple[str, ...]] = None
    ) -> str:
        statements: list[str] = []
        statements.append(f"INSERT INTO {table.name}")
        columns = [column for column in table.get_columns(order)]
        statements.append(_field_list([column.name for column in columns]))
        statements.append("ON DUPLICATE KEY UPDATE")
        value_columns = table.get_value_columns()
        if value_columns:
            defs = [_field_update(column.name) for column in value_columns]
        else:
            defs = [_field_update(key) for key in table.primary_key]
        statements.append(",\n".join(defs))
        statements.append(";")
        return "\n".join(statements)

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


def _field_list(field_ids: list[LocalId]) -> str:
    field_list = ", ".join(str(field_id) for field_id in field_ids)
    value_list = ", ".join("%s" for _ in field_ids)
    if False:
        # compatible with MySQL 8.0.19 and later, slow with aiomysql 0.2.0 and earlier
        return f"({field_list}) VALUES ({value_list}) AS EXCLUDED"
    else:
        # emits a warning with MySQL 8.0.20 and later
        return f"({field_list}) VALUES ({value_list})"


def _field_update(field_id: LocalId) -> str:
    if False:
        # compatible with MySQL 8.0.19 and later, slow with aiomysql 0.2.0 and earlier
        return f"{field_id} = EXCLUDED.{field_id}"
    else:
        # emits a warning with MySQL 8.0.20 and later
        return f"{field_id} = VALUES({field_id})"
