import json
import re
import typing
from typing import Optional

from pysqlsync.base import BaseGenerator, GeneratorOptions
from pysqlsync.formation.object_types import (
    FormationError,
    MutableObject,
    StructType,
    Table,
)
from pysqlsync.formation.py_to_sql import (
    DataclassConverter,
    DataclassConverterOptions,
    EnumMode,
    NamespaceMapping,
    StructMode,
)

_sql_quoted_str_table = str.maketrans(
    {
        "\\": "\\\\",
        "'": "\\'",
        "\b": "\\b",
        "\f": "\\f",
        "\n": "\\n",
        "\r": "\\r",
        "\t": "\\t",
    }
)


def sql_quoted_string(text: str) -> str:
    if re.search(r"[\b\f\n\r\t]", text):
        string = text.translate(_sql_quoted_str_table)
        return f"E'{string}'"
    else:
        string = text.replace("'", "''")
        return f"'{string}'"


class PostgreSQLTable(Table):
    def create_stmt(self) -> str:
        statements: list[str] = []
        statements.append(super().create_stmt())

        # output comments for table and column objects
        if self.description is not None:
            statements.append(
                f"COMMENT ON TABLE {self.name} IS {sql_quoted_string(self.description)};"
            )
        for column in self.columns.values():
            if column.description is not None:
                statements.append(
                    f"COMMENT ON COLUMN {self.name}.{column.name} IS {sql_quoted_string(column.description)};"
                )
        return "\n".join(statements)

    def mutate_stmt(self, src: MutableObject) -> Optional[str]:
        source = typing.cast(Table, src)
        target = self

        statements: list[str] = []
        statement = super().mutate_stmt(src)
        if statement is not None:
            statements.append(statement)

        for target_column in target.columns.values():
            source_column = source.columns.get(target_column.name.id)

            source_desc = (
                source_column.description if source_column is not None else None
            )
            target_desc = target_column.description

            if target_desc is None:
                if source_desc is not None:
                    statements.append(
                        f"COMMENT ON COLUMN {self.name}.{target_column.name} IS NULL;"
                    )
            else:
                if source_desc != target_desc:
                    statements.append(
                        f"COMMENT ON COLUMN {self.name}.{target_column.name} IS {sql_quoted_string(target_desc)};"
                    )

        if target.description is None:
            if source.description is not None:
                statements.append(f"COMMENT ON TABLE {self.name} IS NULL;")
        else:
            if source.description != target.description:
                statements.append(
                    f"COMMENT ON TABLE {self.name} IS {sql_quoted_string(target.description)};"
                )

        return "\n".join(statements)


class PostgreSQLStructType(StructType):
    def create_stmt(self) -> str:
        statements: list[str] = []
        statements.append(super().create_stmt())

        if self.description is not None:
            statements.append(
                f"COMMENT ON TYPE {self.name} IS {sql_quoted_string(self.description)};"
            )
        for member in self.members.values():
            if member.description is not None:
                statements.append(
                    f"COMMENT ON COLUMN {self.name}.{member.name} IS {sql_quoted_string(member.description)};"
                )
        return "\n".join(statements)

    def mutate_stmt(self, src: MutableObject) -> Optional[str]:
        return super().mutate_stmt(src)


class PostgreSQLGenerator(BaseGenerator):
    converter: DataclassConverter

    @property
    def table_class(self) -> type[Table]:
        return PostgreSQLTable

    def __init__(self, options: GeneratorOptions) -> None:
        super().__init__(options)

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
                table_class=PostgreSQLTable,
                struct_class=PostgreSQLStructType,
            )
        )

    def get_table_insert_stmt(self, table: Table) -> str:
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
