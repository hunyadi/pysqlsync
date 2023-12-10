import datetime
import ipaddress
import uuid
from typing import Any, Callable, Optional

from strong_typing.auxiliary import int8, int16, int32, int64
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
from pysqlsync.util.typing import override

from .data_types import (
    OracleIntegerType,
    OracleTimestampType,
    OracleVariableBinaryType,
    OracleVariableCharacterType,
)
from .object_types import OracleObjectFactory


class OracleGenerator(BaseGenerator):
    "Generator for Oracle."

    converter: DataclassConverter

    def __init__(self, options: GeneratorOptions) -> None:
        super().__init__(options, OracleObjectFactory())

        if options.enum_mode is EnumMode.TYPE:
            raise FormationError(
                f"unsupported enum conversion mode for {self.__class__.__name__}: {options.enum_mode}"
            )

        self.converter = DataclassConverter(
            options=DataclassConverterOptions(
                enum_mode=options.enum_mode or EnumMode.INLINE,
                struct_mode=options.struct_mode or StructMode.JSON,
                array_mode=options.array_mode or ArrayMode.JSON,
                qualified_names=False,
                namespaces=NamespaceMapping(options.namespaces),
                foreign_constraints=options.foreign_constraints,
                substitutions={
                    bytes: OracleVariableBinaryType(),
                    datetime.datetime: OracleTimestampType(),
                    int: OracleIntegerType(),
                    int8: OracleIntegerType(),
                    int16: OracleIntegerType(),
                    int32: OracleIntegerType(),
                    int64: OracleIntegerType(),
                    str: OracleVariableCharacterType(),
                    uuid.UUID: OracleVariableBinaryType(16),
                    JsonType: OracleVariableCharacterType(),
                    ipaddress.IPv4Address: OracleVariableBinaryType(4),
                    ipaddress.IPv6Address: OracleVariableBinaryType(16),
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
