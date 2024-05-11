import datetime
import ipaddress
import uuid

from strong_typing.core import JsonType

from pysqlsync.base import BaseGenerator, GeneratorOptions
from pysqlsync.formation.mutation import Mutator
from pysqlsync.formation.object_types import FormationError
from pysqlsync.formation.py_to_sql import (
    ArrayMode,
    DataclassConverter,
    DataclassConverterOptions,
    EnumMode,
    NamespaceMapping,
    StructMode,
)
from pysqlsync.model.data_types import SqlFixedBinaryType
from pysqlsync.util.typing import override

from .data_types import SnowflakeDateTimeType, SnowflakeJsonType
from .object_types import SnowflakeObjectFactory


class SnowflakeGenerator(BaseGenerator):
    "Generator for Snowflake."

    converter: DataclassConverter

    def __init__(self, options: GeneratorOptions) -> None:
        super().__init__(
            options,
            SnowflakeObjectFactory(),
            Mutator(options.synchronization),
        )

        if (
            options.enum_mode is EnumMode.INLINE
            or options.enum_mode is EnumMode.RELATION
            or options.enum_mode is EnumMode.TYPE
        ):
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
                enum_mode=options.enum_mode or EnumMode.CHECK,
                struct_mode=options.struct_mode or StructMode.JSON,
                array_mode=options.array_mode or ArrayMode.JSON,
                namespaces=NamespaceMapping(options.namespaces),
                check_constraints=False,
                foreign_constraints=False,
                initialize_tables=options.initialize_tables,
                substitutions={
                    datetime.datetime: SnowflakeDateTimeType(),
                    uuid.UUID: SqlFixedBinaryType(16),
                    JsonType: SnowflakeJsonType(),
                    ipaddress.IPv4Address: SqlFixedBinaryType(4),
                    ipaddress.IPv6Address: SqlFixedBinaryType(16),
                },
                skip_annotations=options.skip_annotations,
                factory=self.factory,
            )
        )

    @override
    def placeholder(self, index: int) -> str:
        return f":{index}"
