from pysqlsync.base import BaseGenerator, GeneratorOptions
from pysqlsync.formation.object_types import FormationError, Table
from pysqlsync.formation.py_to_sql import (
    DataclassConverter,
    DataclassConverterOptions,
    EnumMode,
    NamespaceMapping,
    StructMode,
)

from .object_types import PostgreSQLObjectFactory


class PostgreSQLGenerator(BaseGenerator):
    "Generator for PostgreSQL."

    converter: DataclassConverter

    def __init__(self, options: GeneratorOptions) -> None:
        super().__init__(options, PostgreSQLObjectFactory())

        if options.enum_mode is EnumMode.INLINE:
            raise FormationError(
                f"unsupported enum conversion mode for {self.__class__.__name__}: {options.enum_mode}"
            )

        self.converter = DataclassConverter(
            options=DataclassConverterOptions(
                enum_mode=options.enum_mode or EnumMode.TYPE,
                struct_mode=options.struct_mode or StructMode.TYPE,
                namespaces=NamespaceMapping(options.namespaces),
                foreign_constraints=options.foreign_constraints,
                skip_annotations=options.skip_annotations,
                factory=self.factory,
            )
        )

    def get_table_merge_stmt(self, table: Table) -> str:
        statements: list[str] = []
        statements.append(f"INSERT INTO {table.name}")
        columns = [column for column in table.columns.values() if not column.identity]
        column_list = ", ".join(str(column.name) for column in columns)
        value_list = ", ".join(f"${index}" for index, _ in enumerate(columns, start=1))
        statements.append(f"({column_list}) VALUES ({value_list})")
        statements.append("ON CONFLICT DO NOTHING")
        return "\n".join(statements)

    def get_table_upsert_stmt(self, table: Table) -> str:
        statements: list[str] = []
        statements.append(f"INSERT INTO {table.name}")
        columns = [column for column in table.columns.values() if not column.identity]
        column_list = ", ".join(str(column.name) for column in columns)
        value_list = ", ".join(f"${index}" for index, _ in enumerate(columns, start=1))
        statements.append(f"({column_list}) VALUES ({value_list})")
        value_columns = table.get_value_columns()
        if value_columns:
            statements.append(f"ON CONFLICT ({table.primary_key}) DO UPDATE SET")
            defs = [
                f"{column.name} = EXCLUDED.{column.name}" for column in value_columns
            ]
            statements.append(",\n".join(defs))
        else:
            statements.append(f"ON CONFLICT ({table.primary_key}) DO NOTHING")
        return "\n".join(statements)
