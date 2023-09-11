from ..base import BaseGenerator
from ..formation.converter import (
    DataclassConverter,
    DataclassConverterOptions,
    NamespaceMapping,
    is_entity_type,
    is_struct_type,
)


class TrinoGenerator(BaseGenerator):
    def get_create_stmt(self) -> str:
        options = DataclassConverterOptions(
            enum_as_type=False, namespaces=NamespaceMapping(self.options.namespaces)
        )
        converter = DataclassConverter(options=options)

        if is_entity_type(self.cls):
            table = converter.dataclass_to_table(self.cls)
            return str(table)
        elif is_struct_type(self.cls):
            struct = converter.dataclass_to_struct(self.cls)
            return str(struct)
        else:
            raise TypeError(
                f"expected: entity (table) or struct; instantiated with: {self.cls}"
            )

    def get_drop_stmt(self, ignore_missing: bool) -> str:
        raise NotImplementedError()

    def get_upsert_stmt(self) -> str:
        raise NotImplementedError()
