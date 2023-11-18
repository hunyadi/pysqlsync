from typing import Optional

from pysqlsync.base import BaseGenerator, GeneratorOptions
from pysqlsync.formation.object_types import Table
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
                skip_annotations=options.skip_annotations,
            )
        )

    @override
    def get_table_merge_stmt(
        self, table: Table, order: Optional[tuple[str, ...]] = None
    ) -> str:
        raise NotImplementedError()

    @override
    def get_table_upsert_stmt(
        self, table: Table, order: Optional[tuple[str, ...]] = None
    ) -> str:
        raise NotImplementedError()

    @override
    def get_table_delete_stmt(self, table: Table) -> str:
        raise NotImplementedError()
