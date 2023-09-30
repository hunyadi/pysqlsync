from strong_typing.inspection import DataclassInstance

from pysqlsync.base import BaseGenerator, GeneratorOptions
from pysqlsync.formation.object_types import Table
from pysqlsync.formation.py_to_sql import (
    DataclassConverter,
    DataclassConverterOptions,
    EnumMode,
    NamespaceMapping,
    StructMode,
)


class TrinoGenerator(BaseGenerator):
    def __init__(self, options: GeneratorOptions) -> None:
        super().__init__(options)
        self.converter = DataclassConverter(
            options=DataclassConverterOptions(
                enum_mode=EnumMode.CHECK,
                struct_mode=StructMode.JSON,
                namespaces=NamespaceMapping(self.options.namespaces),
            )
        )

    def get_table_insert_stmt(self, table: Table) -> str:
        raise NotImplementedError()

    def get_table_upsert_stmt(self, table: Table) -> str:
        raise NotImplementedError()
