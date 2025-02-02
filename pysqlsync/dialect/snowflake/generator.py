import datetime
import ipaddress
import uuid

from strong_typing.core import JsonType

from pysqlsync.base import BaseGenerator, GeneratorOptions
from pysqlsync.formation.mutation import Mutator
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

        self.check_enum_mode(matches=EnumMode.CHECK)
        self.check_struct_mode(matches=StructMode.JSON)
        self.check_array_mode(include=[ArrayMode.JSON, ArrayMode.RELATION])

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
                factory=self.factory,
                skip_annotations=options.skip_annotations,
                auto_default=options.auto_default,
            )
        )

    @override
    def placeholder(self, index: int) -> str:
        return f":{index}"
