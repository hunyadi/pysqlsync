import dataclasses
import re
from typing import Optional

from strong_typing.inspection import DataclassInstance

from pysqlsync.base import BaseGenerator, GeneratorOptions
from pysqlsync.formation.object_types import Catalog
from pysqlsync.formation.py_to_sql import (
    DataclassConverter,
    DataclassConverterOptions,
    NamespaceMapping,
)
from pysqlsync.model.properties import get_primary_key_name


def sql_quoted_id(name: str) -> str:
    escaped_name = name.replace('"', '""')
    return f'"{escaped_name}"'


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
                enum_as_type=True,
                struct_as_type=True,
                namespaces=NamespaceMapping(self.options.namespaces),
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

    def get_quoted_id(self, table: type[DataclassInstance]) -> str:
        return self.converter.create_qualified_id(
            table.__module__, table.__name__
        ).quoted_id

    def get_upsert_stmt(self, table: type[DataclassInstance]) -> str:
        statements: list[str] = []
        statements.append(f"INSERT INTO {self.get_quoted_id(table)}")
        field_names = [field.name for field in dataclasses.fields(table)]
        field_list = ", ".join(sql_quoted_id(field_name) for field_name in field_names)
        value_list = ", ".join(f"${index}" for index in range(1, len(field_names) + 1))
        statements.append(f"({field_list}) VALUES ({value_list})")

        primary_key_name = get_primary_key_name(table)
        statements.append(
            f"ON CONFLICT({sql_quoted_id(primary_key_name)}) DO UPDATE SET"
        )
        defs = [
            f"{sql_quoted_id(field_name)} = EXCLUDED.{sql_quoted_id(field_name)}"
            for field_name in field_names
            if field_name != primary_key_name
        ]
        statements.append(",\n".join(defs))
        return "\n".join(statements)
