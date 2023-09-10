import dataclasses
import re

from pysqlsync.model.id_types import QualifiedId

from ..base import BaseGenerator
from ..formation.converter import (
    DataclassConverter,
    DataclassConverterOptions,
    NamespaceMapping,
    is_entity_type,
    is_struct_type,
    quote,
)
from ..model.properties import get_primary_key_name


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


class Generator(BaseGenerator):
    def get_create_stmt(self) -> str:
        options = DataclassConverterOptions(
            enum_as_type=False, namespaces=NamespaceMapping(self.options.namespaces)
        )
        converter = DataclassConverter(options=options)

        # output comments for table and column objects
        output: list[str] = []
        if is_entity_type(self.cls):
            table = converter.dataclass_to_table(self.cls)

            output.append(str(table))
            if table.description is not None:
                output.append(
                    f"COMMENT ON TABLE {table.name} IS {quote(table.description)};"
                )
            for column in table.columns:
                if column.description is not None:
                    output.append(
                        f"COMMENT ON COLUMN {table.name}.{column.name} IS {quote(column.description)};"
                    )
        elif is_struct_type(self.cls):
            struct = converter.dataclass_to_struct(self.cls)

            output.append(str(struct))
            if struct.description is not None:
                output.append(
                    f"COMMENT ON TYPE {struct.name} IS {quote(struct.description)};"
                )
            for member in struct.members:
                if member.description is not None:
                    output.append(
                        f"COMMENT ON COLUMN {struct.name}.{member.name} IS {quote(member.description)};"
                    )

        return "\n".join(output)

    def get_drop_stmt(self, ignore_missing: bool) -> str:
        namespaces = NamespaceMapping(self.options.namespaces)
        table = QualifiedId(namespaces.get(self.cls.__module__), self.cls.__name__)

        if ignore_missing:
            return f"DROP TABLE IF EXISTS {table}"
        else:
            return f"DROP TABLE {table}"

    def get_upsert_stmt(self) -> str:
        statements: list[str] = []
        statements.append(f"INSERT INTO {sql_quoted_id(self.cls.__name__)}")
        field_names = [field.name for field in dataclasses.fields(self.cls)]
        field_list = ", ".join(sql_quoted_id(field_name) for field_name in field_names)
        value_list = ", ".join(f"${index}" for index in range(1, len(field_names) + 1))
        statements.append(f"({field_list}) VALUES ({value_list})")

        primary_key_name = get_primary_key_name(self.cls)
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
