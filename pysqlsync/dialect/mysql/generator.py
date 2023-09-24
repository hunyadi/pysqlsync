import dataclasses
import uuid
from typing import Any, Callable

from pysqlsync.base import BaseGenerator, GeneratorOptions
from pysqlsync.formation.object_types import Table
from pysqlsync.formation.py_to_sql import (
    DataclassConverter,
    DataclassConverterOptions,
    NamespaceMapping,
)
from pysqlsync.model.data_types import SqlFixedBinaryType
from pysqlsync.model.properties import get_primary_key_name


def sql_quoted_id(name: str) -> str:
    escaped_name = name.replace('"', '""')
    return f'"{escaped_name}"'


class MySQLGenerator(BaseGenerator):
    converter: DataclassConverter
    table: Table

    def __init__(self, cls: type, options: GeneratorOptions) -> None:
        super().__init__(cls, options)
        self.converter = DataclassConverter(
            options=DataclassConverterOptions(
                enum_as_type=False,
                struct_as_type=False,
                qualified_names=False,
                namespaces=NamespaceMapping(self.options.namespaces),
                substitutions={uuid.UUID: SqlFixedBinaryType(16)},
            )
        )
        self.table = self.converter.dataclass_to_table(self.cls)

    def get_create_stmt(self) -> str:
        return str(self.table)

    def get_drop_stmt(self) -> str:
        return self.table.drop_stmt()

    def get_quoted_id(self) -> str:
        return self.converter.create_qualified_id(
            self.cls.__module__, self.cls.__name__
        ).quoted_id

    def get_upsert_stmt(self) -> str:
        statements: list[str] = []
        statements.append(f"INSERT INTO {self.get_quoted_id()}")
        field_names = [field.name for field in dataclasses.fields(self.cls)]
        field_list = ", ".join(sql_quoted_id(field_name) for field_name in field_names)
        value_list = ", ".join(f"%s" for field_name in field_names)
        statements.append(f"({field_list}) VALUES ({value_list}) AS EXCLUDED")

        primary_key_name = get_primary_key_name(self.cls)
        statements.append(f"ON DUPLICATE KEY UPDATE")
        defs = [
            f"{sql_quoted_id(field_name)} = EXCLUDED.{sql_quoted_id(field_name)}"
            for field_name in field_names
            if field_name != primary_key_name
        ]
        statements.append(",\n".join(defs))
        return "\n".join(statements)

    def get_extractor(self, field_name: str, field_type: type) -> Callable[[Any], Any]:
        if field_type is uuid.UUID:
            return lambda obj: getattr(obj, field_name).bytes

        return super().get_extractor(field_name, field_type)
