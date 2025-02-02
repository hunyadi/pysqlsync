import datetime
import ipaddress
import uuid
from typing import Any, Callable, Optional

from strong_typing.auxiliary import int8, int16, int32, int64
from strong_typing.core import JsonType

from pysqlsync.base import BaseGenerator, GeneratorOptions
from pysqlsync.dialect.oracle.mutation import OracleMutator
from pysqlsync.formation.inspection import is_ip_address_type
from pysqlsync.formation.object_types import Column
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
    OracleTimeType,
    OracleVariableBinaryType,
    OracleVariableCharacterType,
)
from .object_types import OracleObjectFactory

MIN_DATETIME = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
MIN_DATE = datetime.date.min


class OracleGenerator(BaseGenerator):
    "Generator for Oracle."

    converter: DataclassConverter

    def __init__(self, options: GeneratorOptions) -> None:
        super().__init__(
            options, OracleObjectFactory(), OracleMutator(options.synchronization)
        )

        self.check_enum_mode(exclude=[EnumMode.TYPE, EnumMode.INLINE])
        self.check_struct_mode(matches=StructMode.JSON)
        self.check_array_mode(include=[ArrayMode.JSON, ArrayMode.RELATION])

        self.converter = DataclassConverter(
            options=DataclassConverterOptions(
                enum_mode=options.enum_mode or EnumMode.RELATION,
                struct_mode=options.struct_mode or StructMode.JSON,
                array_mode=options.array_mode or ArrayMode.JSON,
                qualified_names=False,
                namespaces=NamespaceMapping(options.namespaces),
                foreign_constraints=options.foreign_constraints,
                initialize_tables=options.initialize_tables,
                substitutions={
                    bytes: OracleVariableBinaryType(),
                    datetime.time: OracleTimeType(),
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
                factory=self.factory,
                skip_annotations=options.skip_annotations,
                auto_default=options.auto_default,
            )
        )

    @override
    def get_current_schema_stmt(self) -> str:
        return "SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')"

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
        elif field_type is datetime.time:
            return (
                lambda obj: datetime.datetime.combine(
                    MIN_DATE, getattr(obj, field_name)
                )
                - MIN_DATETIME
            )

        return super().get_field_extractor(column, field_name, field_type)

    @override
    def get_value_transformer(
        self, column: Column, field_type: type
    ) -> Optional[Callable[[Any], Any]]:
        if field_type is uuid.UUID:
            return lambda field: field.bytes
        elif is_ip_address_type(field_type):
            return lambda field: field.packed
        elif field_type is datetime.time:
            return (
                lambda field: datetime.datetime.combine(datetime.date.min, field)
                - datetime.datetime.min
            )

        return super().get_value_transformer(column, field_type)
