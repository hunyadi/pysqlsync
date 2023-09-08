import dataclasses

from ..base import BaseGenerator
from ..formation.converter import (
    DataclassConverter,
    DataclassConverterOptions,
    is_entity_type,
    is_struct_type,
    quote,
)
from ..model.properties import get_primary_key_name


def sql_quoted_id(name: str) -> str:
    escaped_name = name.replace('"', '""')
    return f'"{escaped_name}"'


def sql_quoted_string(value: str) -> str:
    escaped_value = value.replace("'", "''")
    return f"'{escaped_value}'"


class Generator(BaseGenerator):
    def get_create_stmt(self) -> str:
        options = DataclassConverterOptions(enum_as_type=False)
        converter = DataclassConverter(options=options)

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
