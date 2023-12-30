import ipaddress
import uuid
from typing import Any, Callable, Optional

from strong_typing.core import JsonType

from pysqlsync.base import BaseGenerator, GeneratorOptions
from pysqlsync.formation.inspection import is_ip_address_type
from pysqlsync.formation.object_types import Column, FormationError
from pysqlsync.formation.py_to_sql import (
    ArrayMode,
    DataclassConverter,
    DataclassConverterOptions,
    EnumMode,
    NamespaceMapping,
    StructMode,
)
from pysqlsync.model.data_types import SqlVariableCharacterType
from pysqlsync.util.typing import override

from ..postgresql.mutation import PostgreSQLMutator
from ..postgresql.object_types import PostgreSQLObjectFactory
from .data_types import RedshiftVariableBinaryType


class RedshiftGenerator(BaseGenerator):
    "Generator for AWS Redshift."

    converter: DataclassConverter

    def __init__(self, options: GeneratorOptions) -> None:
        super().__init__(
            options,
            PostgreSQLObjectFactory(),
            PostgreSQLMutator(options.synchronization),
        )

        if options.enum_mode is EnumMode.INLINE or options.enum_mode is EnumMode.TYPE:
            raise FormationError(
                f"unsupported enum conversion mode for {self.__class__.__name__}: {options.enum_mode}"
            )
        if options.struct_mode is StructMode.TYPE:
            raise FormationError(
                f"unsupported struct conversion mode for {self.__class__.__name__}: {options.struct_mode}"
            )
        if options.array_mode is ArrayMode.ARRAY:
            raise FormationError(
                f"unsupported array conversion mode for {self.__class__.__name__}: {options.array_mode}"
            )
        self.converter = DataclassConverter(
            options=DataclassConverterOptions(
                enum_mode=options.enum_mode or EnumMode.RELATION,
                struct_mode=options.struct_mode or StructMode.JSON,
                array_mode=options.array_mode or ArrayMode.JSON,
                namespaces=NamespaceMapping(options.namespaces),
                foreign_constraints=False,
                substitutions={
                    uuid.UUID: RedshiftVariableBinaryType(16),
                    JsonType: SqlVariableCharacterType(),
                    ipaddress.IPv4Address: RedshiftVariableBinaryType(4),
                    ipaddress.IPv6Address: RedshiftVariableBinaryType(16),
                },
                skip_annotations=options.skip_annotations,
                factory=self.factory,
            )
        )

    @override
    def placeholder(self, index: int) -> str:
        return f":{index}"

    @override
    def get_field_extractor(
        self, column: Column, field_name: str, field_type: type
    ) -> Callable[[Any], Any]:
        if field_type is uuid.UUID:
            return lambda obj: getattr(obj, field_name).bytes
        elif is_ip_address_type(field_type):
            return lambda obj: getattr(obj, field_name).packed

        return super().get_field_extractor(column, field_name, field_type)

    @override
    def get_value_transformer(
        self, column: Column, field_type: type
    ) -> Optional[Callable[[Any], Any]]:
        if field_type is uuid.UUID:
            return lambda field: field.bytes
        elif is_ip_address_type(field_type):
            return lambda field: field.packed

        return super().get_value_transformer(column, field_type)
