import datetime
import ipaddress
import uuid
from typing import Any, Callable, Optional

from strong_typing.core import JsonType

from pysqlsync.base import BaseGenerator, GeneratorOptions
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
from pysqlsync.model.data_types import SqlFixedBinaryType
from pysqlsync.util.typing import override

from .data_types import MSSQLBooleanType, MSSQLDateTimeType, MSSQLVariableCharacterType
from .mutation import MSSQLMutator
from .object_types import MSSQLObjectFactory


class MSSQLGenerator(BaseGenerator):
    """
    Generator for Microsoft SQL Server (T-SQL).

    Assumes a UTF-8 collation and `SET ANSI_DEFAULTS ON`. UTF-8 collation makes `varchar` store UTF-8 characters.
    """

    def __init__(self, options: GeneratorOptions) -> None:
        super().__init__(
            options, MSSQLObjectFactory(), MSSQLMutator(options.synchronization)
        )

        self.check_enum_mode(exclude=[EnumMode.TYPE, EnumMode.INLINE])
        self.check_struct_mode(matches=StructMode.JSON)
        self.check_array_mode(include=[ArrayMode.JSON, ArrayMode.RELATION])

        self.converter = DataclassConverter(
            options=DataclassConverterOptions(
                enum_mode=options.enum_mode or EnumMode.RELATION,
                struct_mode=options.struct_mode or StructMode.JSON,
                array_mode=options.array_mode or ArrayMode.JSON,
                namespaces=NamespaceMapping(options.namespaces),
                foreign_constraints=options.foreign_constraints,
                initialize_tables=options.initialize_tables,
                substitutions={
                    bool: MSSQLBooleanType(),
                    datetime.datetime: MSSQLDateTimeType(),
                    str: MSSQLVariableCharacterType(),
                    uuid.UUID: SqlFixedBinaryType(16),
                    JsonType: MSSQLVariableCharacterType(),
                    ipaddress.IPv4Address: SqlFixedBinaryType(4),
                    ipaddress.IPv6Address: SqlFixedBinaryType(16),
                },
                factory=self.factory,
                skip_annotations=options.skip_annotations,
                auto_default=options.auto_default,
            )
        )

    @override
    def get_current_schema_stmt(self) -> str:
        return "SCHEMA_NAME()"

    @override
    def placeholder(self, index: int) -> str:
        return "?"

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
