from strong_typing.inspection import DataclassInstance

from pysqlsync.base import BaseGenerator, GeneratorOptions
from pysqlsync.formation.py_to_sql import (
    DataclassConverter,
    DataclassConverterOptions,
    NamespaceMapping,
)


class TrinoGenerator(BaseGenerator):
    def __init__(self, options: GeneratorOptions) -> None:
        super().__init__(options)
        self.converter = DataclassConverter(
            options=DataclassConverterOptions(
                enum_as_type=False,
                struct_as_type=False,
                namespaces=NamespaceMapping(self.options.namespaces),
            )
        )

    def get_upsert_stmt(self, table: type[DataclassInstance]) -> str:
        raise NotImplementedError()
