import uuid
from typing import Any, Callable

from pysqlsync.base import BaseGenerator, GeneratorOptions
from pysqlsync.formation.object_types import Catalog, Column, Table, constant, quote
from pysqlsync.formation.py_to_sql import (
    DataclassConverter,
    DataclassConverterOptions,
    EnumMode,
    NamespaceMapping,
    StructMode,
)
from pysqlsync.model.data_types import SqlFixedBinaryType
from pysqlsync.model.id_types import LocalId


class MySQLColumn(Column):
    @property
    def data_spec(self) -> str:
        nullable = " NOT NULL" if not self.nullable else ""
        default = (
            f" DEFAULT {constant(self.default)}" if self.default is not None else ""
        )
        identity = " AUTO_INCREMENT" if self.identity else ""
        return f"{self.data_type}{nullable}{default}{identity}"

    def mutate_column_stmt(target: Column, source: Column) -> list[str]:
        statements: list[str] = []
        if (
            source.data_type != target.data_type
            or source.nullable != target.nullable
            or source.default != target.default
            or source.identity != target.identity
        ):
            statements.append(f"MODIFY COLUMN {source.name} {target.data_spec}")
        return statements


class MySQLGenerator(BaseGenerator):
    @property
    def column_class(self) -> type[Column]:
        return MySQLColumn

    def __init__(self, options: GeneratorOptions) -> None:
        super().__init__(options)
        self.converter = DataclassConverter(
            options=DataclassConverterOptions(
                enum_mode=EnumMode.CHECK,
                struct_mode=StructMode.JSON,
                qualified_names=False,
                namespaces=NamespaceMapping(self.options.namespaces),
                substitutions={uuid.UUID: SqlFixedBinaryType(16)},
                column_class=MySQLColumn,
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
                        f"ALTER TABLE {table.name} COMMENT = {quote(table.description)};"
                    )
                for column in table.columns.values():
                    if column.description is None:
                        continue

                    statements.append(
                        f"ALTER TABLE {table.name} MODIFY COLUMN {column.name} {column.data_spec} COMMENT = {quote(column.description)};"
                    )
        return "\n".join(statements)

    def get_table_insert_stmt(self, table: Table) -> str:
        statements: list[str] = []
        statements.append(f"INSERT IGNORE INTO {table.name}")
        columns = [column for column in table.columns.values() if not column.identity]
        column_list = ", ".join(str(column.name) for column in columns)
        value_list = ", ".join(f"%s" for column in columns)
        statements.append(f"({column_list}) VALUES ({value_list})")
        return "\n".join(statements)

    def get_table_upsert_stmt(self, table: Table) -> str:
        statements: list[str] = []
        statements.append(f"INSERT INTO {table.name}")
        columns = [column for column in table.columns.values() if not column.identity]
        statements.append(_field_list([column.name for column in columns]))
        statements.append(f"ON DUPLICATE KEY UPDATE")
        defs = [_field_update(column.name) for column in table.get_value_columns()]
        statements.append(",\n".join(defs))
        return "\n".join(statements)

    def get_field_extractor(
        self, field_name: str, field_type: type
    ) -> Callable[[Any], Any]:
        if field_type is uuid.UUID:
            return lambda obj: getattr(obj, field_name).bytes

        return super().get_field_extractor(field_name, field_type)


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
