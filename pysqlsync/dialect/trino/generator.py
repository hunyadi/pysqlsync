from pysqlsync.base import BaseGenerator, GeneratorOptions
from pysqlsync.formation.py_to_sql import (
    ArrayMode,
    DataclassConverter,
    DataclassConverterOptions,
    EnumMode,
    NamespaceMapping,
    StructMode,
)
from pysqlsync.util.typing import override


class TrinoGenerator(BaseGenerator):
    def __init__(self, options: GeneratorOptions) -> None:
        super().__init__(options)
        self.converter = DataclassConverter(
            options=DataclassConverterOptions(
                enum_mode=options.enum_mode or EnumMode.CHECK,
                struct_mode=options.struct_mode or StructMode.JSON,
                array_mode=options.array_mode or ArrayMode.JSON,
                namespaces=NamespaceMapping(options.namespaces),
                foreign_constraints=options.foreign_constraints,
                initialize_tables=options.initialize_tables,
                skip_annotations=options.skip_annotations,
                auto_default=options.auto_default,
            )
        )

    @override
    def placeholder(self, index: int) -> str:
        raise NotImplementedError("operation not supported for Trino")
