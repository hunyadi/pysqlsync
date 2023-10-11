import datetime
import ipaddress
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Optional

from pysqlsync.base import BaseGenerator, GeneratorOptions
from pysqlsync.formation.object_types import (
    Catalog,
    Column,
    FormationError,
    Table,
    constant,
    quote,
)
from pysqlsync.formation.py_to_sql import (
    ArrayMode,
    DataclassConverter,
    DataclassConverterOptions,
    EnumMode,
    NamespaceMapping,
    StructMode,
)
from pysqlsync.model.data_types import SqlFixedBinaryType
from pysqlsync.model.id_types import LocalId

from .data_types import MySQLDateTimeType


def _description(description: str) -> str:
    if "\n" in description:
        description = description[: description.index("\n")]

    return quote(description)


@dataclass(eq=True)
class MySQLColumn(Column):
    @property
    def data_spec(self) -> str:
        nullable = " NOT NULL" if not self.nullable else ""
        default = (
            f" DEFAULT {constant(self.default)}" if self.default is not None else ""
        )
        identity = " AUTO_INCREMENT" if self.identity else ""
        description = (
            f" COMMENT {self.comment()}" if self.description is not None else ""
        )
        return f"{self.data_type}{nullable}{default}{identity}{description}"

    def mutate_column_stmt(target: Column, source: Column) -> list[str]:
        statements: list[str] = []
        if (
            source.data_type != target.data_type
            or source.nullable != target.nullable
            or source.default != target.default
            or source.identity != target.identity
            or source.description != target.description
        ):
            statements.append(f"MODIFY COLUMN {source.name} {target.data_spec}")
        return statements

    def comment(self) -> Optional[str]:
        if self.description is not None:
            description = (
                self.description
                if "\n" not in self.description
                else self.description[: self.description.index("\n")]
            )

            if len(description) > 1024:
                raise FormationError(
                    f"comment for column {self.name} too long, expected: maximum 1024; got: {len(description)}"
                )

            return quote(description)
        else:
            return None


class MySQLGenerator(BaseGenerator):
    @property
    def column_class(self) -> type[Column]:
        return MySQLColumn

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
                enum_mode=options.enum_mode or EnumMode.INLINE,
                struct_mode=options.struct_mode or StructMode.JSON,
                array_mode=ArrayMode.JSON,
                qualified_names=False,
                namespaces=NamespaceMapping(options.namespaces),
                foreign_constraints=options.foreign_constraints,
                substitutions={
                    datetime.datetime: MySQLDateTimeType(),
                    uuid.UUID: SqlFixedBinaryType(16),
                    ipaddress.IPv4Address: SqlFixedBinaryType(4),
                    ipaddress.IPv6Address: SqlFixedBinaryType(16),
                },
                column_class=MySQLColumn,
                skip_annotations=options.skip_annotations,
            )
        )

    def get_mutate_stmt(self, target: Catalog) -> str:
        statements: list[str] = []
        target_statements = super().get_mutate_stmt(target)
        if target_statements is not None:
            statements.append(target_statements)

        for namespace in target.namespaces.values():
            for table in namespace.tables.values():
                if table.description is not None:
                    statements.append(
                        f"ALTER TABLE {table.name} COMMENT = {_description(table.description)};"
                    )

        return "\n".join(statements)

    def get_table_insert_stmt(self, table: Table) -> str:
        statements: list[str] = []
        statements.append(f"INSERT INTO {table.name}")
        columns = [column for column in table.columns.values() if not column.identity]
        column_list = ", ".join(str(column.name) for column in columns)
        value_list = ", ".join(f"%s" for column in columns)
        statements.append(f"({column_list}) VALUES ({value_list})")
        statements.append(
            f"ON DUPLICATE KEY UPDATE {table.primary_key} = {table.primary_key}"
        )
        return "\n".join(statements)

    def get_table_upsert_stmt(self, table: Table) -> str:
        statements: list[str] = []
        statements.append(f"INSERT INTO {table.name}")
        columns = [column for column in table.columns.values() if not column.identity]
        statements.append(_field_list([column.name for column in columns]))
        value_columns = table.get_value_columns()
        statements.append(f"ON DUPLICATE KEY UPDATE")
        if value_columns:
            defs = [_field_update(column.name) for column in value_columns]
            statements.append(",\n".join(defs))
        else:
            statements.append(_field_update(table.primary_key))

        return "\n".join(statements)

    def get_field_extractor(
        self, field_name: str, field_type: type
    ) -> Callable[[Any], Any]:
        if field_type is uuid.UUID:
            return lambda obj: getattr(obj, field_name).bytes

        return super().get_field_extractor(field_name, field_type)

    def get_value_transformer(
        self, column: Column, field_type: type
    ) -> Optional[Callable[[Any], Any]]:
        if field_type is uuid.UUID:
            return lambda field: field.bytes
        elif field_type is ipaddress.IPv4Address or field_type is ipaddress.IPv6Address:
            return lambda field: field.packed

        return super().get_value_transformer(column, field_type)


def _field_list(field_ids: list[LocalId]) -> str:
    field_list = ", ".join(str(field_id) for field_id in field_ids)
    value_list = ", ".join(f"%s" for _ in field_ids)
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
