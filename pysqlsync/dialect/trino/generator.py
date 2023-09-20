from pysqlsync.base import BaseGenerator, GeneratorOptions
from pysqlsync.formation.object_types import Table
from pysqlsync.formation.py_to_sql import (
    DataclassConverter,
    DataclassConverterOptions,
    NamespaceMapping,
)


class TrinoGenerator(BaseGenerator):
    table: Table

    def __init__(self, cls: type, options: GeneratorOptions) -> None:
        super().__init__(cls, options)
        converter = DataclassConverter(
            options=DataclassConverterOptions(
                enum_as_type=False,
                struct_as_type=False,
                namespaces=NamespaceMapping(self.options.namespaces),
            )
        )
        self.table = converter.dataclass_to_table(self.cls)

    def get_create_stmt(self) -> str:
        return str(self.table)

    def get_drop_stmt(self) -> str:
        raise NotImplementedError()

    def get_upsert_stmt(self) -> str:
        raise NotImplementedError()
