import re
from typing import Optional

from pysqlsync.base import BaseGenerator, GeneratorOptions
from pysqlsync.formation.object_types import Catalog, Table
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


class PostgreSQLGenerator(BaseGenerator):
    converter: DataclassConverter

    def __init__(self, options: GeneratorOptions) -> None:
        super().__init__(options)
        self.converter = DataclassConverter(
            options=DataclassConverterOptions(
                enum_mode=options.enum_mode or EnumMode.TYPE,
                struct_mode=StructMode.TYPE,
                namespaces=NamespaceMapping(self.options.namespaces),
                skip_annotations=options.skip_annotations,
            )
        )

    def get_mutate_stmt(self, target: Catalog) -> Optional[str]:
        statements: list[str] = []
        target_statements = super().get_mutate_stmt(target)
        if target_statements is not None:
            statements.append(target_statements)

        for namespace in target.namespaces.values():
            for table in namespace.tables.values():
                # output comments for table and column objects
                if table.description is not None:
                    statements.append(
                        f"COMMENT ON TABLE {table.name} IS {sql_quoted_string(table.description)};"
                    )
                for column in table.columns.values():
                    if column.description is not None:
                        statements.append(
                            f"COMMENT ON COLUMN {table.name}.{column.name} IS {sql_quoted_string(column.description)};"
                        )

            for struct in namespace.structs.values():
                if struct.description is not None:
                    statements.append(
                        f"COMMENT ON TYPE {struct.name} IS {sql_quoted_string(struct.description)};"
                    )
                for member in struct.members.values():
                    if member.description is not None:
                        statements.append(
                            f"COMMENT ON COLUMN {struct.name}.{member.name} IS {sql_quoted_string(member.description)};"
                        )

        return "\n".join(statements)

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
        statements.append(f"ON CONFLICT ({table.primary_key}) DO UPDATE SET")
        defs = [
            f"{column.name} = EXCLUDED.{column.name}"
            for column in table.get_value_columns()
        ]
        statements.append(",\n".join(defs))
        return "\n".join(statements)
