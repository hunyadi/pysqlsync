import dataclasses

from ..base import BaseGenerator
from ..formation.converter import DataclassConverter, DataclassConverterOptions
from ..model.properties import get_primary_key_name


def sql_quoted_id(name: str) -> str:
    escaped_name = name.replace('"', '""')
    return f'"{escaped_name}"'


def sql_quoted_string(value: str) -> str:
    escaped_value = value.replace("'", "''")
    return f"'{escaped_value}'"


class Generator(BaseGenerator):
    def get_create_table_stmt(self) -> str:
        options = DataclassConverterOptions(enum_as_type=False)
        converter = DataclassConverter(options=options)
        table = converter.dataclass_to_table(self.cls)
        return str(table)

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
