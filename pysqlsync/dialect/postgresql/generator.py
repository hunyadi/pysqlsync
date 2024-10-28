from typing import Optional

from strong_typing.core import JsonType

from pysqlsync.base import BaseGenerator, GeneratorOptions
from pysqlsync.formation.object_types import Table
from pysqlsync.formation.py_to_sql import (
    ArrayMode,
    DataclassConverter,
    DataclassConverterOptions,
    EnumMode,
    NamespaceMapping,
    StructMode,
)
from pysqlsync.util.typing import override

from .data_types import PostgreSQLJsonType
from .mutation import PostgreSQLMutator
from .object_types import PostgreSQLObjectFactory


class PostgreSQLGenerator(BaseGenerator):
    "Generator for PostgreSQL."

    converter: DataclassConverter

    def __init__(self, options: GeneratorOptions) -> None:
        super().__init__(
            options,
            PostgreSQLObjectFactory(),
            PostgreSQLMutator(options.synchronization),
        )

        self.check_enum_mode(exclude=[EnumMode.INLINE])
        self.check_struct_mode(exclude=[StructMode.INLINE])

        self.converter = DataclassConverter(
            options=DataclassConverterOptions(
                enum_mode=options.enum_mode or EnumMode.TYPE,
                struct_mode=options.struct_mode or StructMode.TYPE,
                array_mode=options.array_mode or ArrayMode.ARRAY,
                unique_constraint_names=False,
                namespaces=NamespaceMapping(options.namespaces),
                foreign_constraints=options.foreign_constraints,
                initialize_tables=options.initialize_tables,
                substitutions={
                    JsonType: PostgreSQLJsonType(),
                },
                factory=self.factory,
                skip_annotations=options.skip_annotations,
                auto_default=options.auto_default,
            )
        )

    @override
    def placeholder(self, index: int) -> str:
        return f"${index}"

    @override
    def get_table_insert_stmt(
        self, table: Table, order: Optional[tuple[str, ...]] = None
    ) -> str:
        statements: list[str] = []
        statements.append(f"INSERT INTO {table.name}")
        columns = [column for column in table.get_columns(order) if not column.identity]
        column_list = ", ".join(str(column.name) for column in columns)
        value_list = ", ".join(
            self.placeholder(index) for index, _ in enumerate(columns, start=1)
        )
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
        value_list = ", ".join(
            self.placeholder(index) for index, _ in enumerate(columns, start=1)
        )
        statements.append(f"({column_list}) VALUES ({value_list})")
        statements.append("ON CONFLICT DO NOTHING")
        statements.append(";")
        return "\n".join(statements)

    @override
    def get_table_upsert_stmt(
        self, table: Table, order: Optional[tuple[str, ...]] = None
    ) -> str:
        statements: list[str] = []
        statements.append(f"INSERT INTO {table.name}")
        columns = [column for column in table.get_columns(order)]
        column_list = ", ".join(str(column.name) for column in columns)
        value_list = ", ".join(
            self.placeholder(index) for index, _ in enumerate(columns, start=1)
        )
        statements.append(f"({column_list}) VALUES ({value_list})")
        value_columns = table.get_value_columns()
        keys = ", ".join(str(key) for key in table.primary_key)
        if value_columns:
            statements.append(f"ON CONFLICT ({keys}) DO UPDATE SET")
            defs = [
                f"{column.name} = EXCLUDED.{column.name}" for column in value_columns
            ]
            statements.append(",\n".join(defs))
        else:
            statements.append(f"ON CONFLICT ({keys}) DO NOTHING")
        statements.append(";")
        return "\n".join(statements)
