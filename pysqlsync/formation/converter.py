import dataclasses
import datetime
import decimal
import inspect
import ipaddress
import sys
import types
import typing
import uuid
from typing import Iterable, Optional, TypeGuard

from strong_typing.auxiliary import (
    float32,
    float64,
    int8,
    int16,
    int32,
    int64,
    uint8,
    uint16,
    uint32,
    uint64,
)
from strong_typing.inspection import (
    DataclassInstance,
    enum_value_types,
    is_dataclass_type,
    is_generic_list,
    is_type_enum,
    is_type_optional,
    unwrap_annotated_type,
    unwrap_generic_list,
    unwrap_optional_type,
)

from ..model.data_types import *
from ..model.properties import get_field_properties
from .object_types import (
    CheckConstraint,
    Column,
    Constraint,
    EnumType,
    ForeignConstraint,
    LocalId,
    Namespace,
    QualifiedId,
    StructMember,
    StructType,
    Table,
    quote,
)


@dataclass
class DataclassField:
    name: str
    type: type


def dataclass_fields(cls: type[DataclassInstance]) -> Iterable[DataclassField]:
    for field in dataclasses.fields(cls):
        if isinstance(field.type, str):
            # evaluate data-class fields whose type annotation is a string
            field_type = eval(
                field.type, sys.modules[cls.__module__].__dict__, locals()
            )
        else:
            field_type = field.type

        yield DataclassField(field.name, field_type)


def is_simple_type(typ: type) -> bool:
    "True if the type is not a composite or user-defined type."

    if (
        typ is bool
        or typ is int
        or typ is float
        or typ is str
        or typ is decimal.Decimal
        or typ is datetime.datetime
        or typ is datetime.date
        or typ is datetime.time
        or typ is datetime.timedelta
        or typ is uuid.UUID
    ):
        return True
    if is_type_enum(typ):
        return True
    if is_type_optional(typ):
        typ = unwrap_optional_type(typ)
        return is_simple_type(typ)
    return False


def is_entity_type(typ: type) -> TypeGuard[type[DataclassInstance]]:
    if not is_dataclass_type(typ):
        return False

    return _dataclass_has_primary_key(typ)


def is_struct_type(typ: type) -> TypeGuard[type[DataclassInstance]]:
    if not is_dataclass_type(typ):
        return False

    return not _dataclass_has_primary_key(typ)


def _dataclass_has_primary_key(typ: type[DataclassInstance]) -> bool:
    for field in dataclass_fields(typ):
        props = get_field_properties(field.type)
        if props.is_primary:
            return True

    return False


def _dataclass_primary_key_name(typ: type[DataclassInstance]) -> LocalId:
    for field in dataclass_fields(typ):
        props = get_field_properties(field.type)
        if props.is_primary:
            return LocalId(field.name)

    raise TypeError(f"table {typ.__name__} lacks primary key")


def _dataclass_primary_key_type(typ: type[DataclassInstance]) -> type:
    "Extracts the primary key data type from a dataclass."

    for field in dataclass_fields(typ):
        props = get_field_properties(field.type)
        if props.is_primary:
            return props.field_type

    raise TypeError(f"table {typ.__name__} lacks primary key")


class DataclassConverter:
    enum_as_type: bool
    extra_numeric_types: bool = False

    def __init__(self, *, enum_as_type: bool = True):
        self.enum_as_type = enum_as_type

    def member_to_sql_data_type(self, typ: type, cls: type) -> SqlDataType:
        """
        Maps a native Python type into a SQL data type.

        :param typ: The type to convert, typically a dataclass member type.
        :param cls: The context for the type, typically the dataclass in which the member is defined.
        """

        if typ is bool:
            return SqlBooleanType()
        if typ is int:
            return SqlIntegerType(8)
        if typ is float:
            return SqlDoubleType()
        if typ is int8:
            if self.extra_numeric_types:
                return SqlUserDefinedType("int1")  # PostgreSQL extension required
            else:
                return SqlIntegerType(2)
        if typ is int16:
            return SqlIntegerType(2)
        if typ is int32:
            return SqlIntegerType(4)
        if typ is int64:
            return SqlIntegerType(8)
        if typ is uint8:
            if self.extra_numeric_types:
                return SqlUserDefinedType("uint1")  # PostgreSQL extension required
            else:
                return SqlIntegerType(2)
        if typ is uint16:
            if self.extra_numeric_types:
                return SqlUserDefinedType("uint2")  # PostgreSQL extension required
            else:
                return SqlIntegerType(2)
        if typ is uint32:
            if self.extra_numeric_types:
                return SqlUserDefinedType("uint4")  # PostgreSQL extension required
            else:
                return SqlIntegerType(4)
        if typ is uint64:
            if self.extra_numeric_types:
                return SqlUserDefinedType("uint8")  # PostgreSQL extension required
            else:
                return SqlIntegerType(8)
        if typ is float32:
            return SqlFloatType()
        if typ is float64:
            return SqlDoubleType()
        if typ is str:
            return SqlCharacterType()
        if typ is decimal.Decimal:
            return SqlDecimalType()
        if typ is datetime.datetime:
            return SqlTimestampType()
        if typ is datetime.date:
            return SqlDateType()
        if typ is datetime.time:
            return SqlTimeType()
        if typ is datetime.timedelta:
            return SqlIntervalType()
        if typ is uuid.UUID:
            return SqlUuidType()
        if typ is ipaddress.IPv4Address or typ is ipaddress.IPv6Address:
            return SqlUserDefinedType("inet")  # PostgreSQL extension required

        metadata = getattr(typ, "__metadata__", None)
        if metadata:
            inner_type = unwrap_annotated_type(typ)
            if inner_type is str:
                sql_type: SqlDataType = SqlCharacterType()
            elif inner_type is decimal.Decimal:
                sql_type = SqlDecimalType()
            elif inner_type is datetime.datetime:
                sql_type = SqlTimestampType()
            elif inner_type is datetime.time:
                sql_type = SqlTimeType()
            else:
                raise TypeError(f"unsupported annotated Python type: {typ}")

            for meta in metadata:
                sql_type.parse_meta(meta)

            return sql_type

        if is_dataclass_type(typ):
            if _dataclass_has_primary_key(typ):
                # a many-to-one relationship to another entity class
                key_field_type = _dataclass_primary_key_type(typ)
                return self.member_to_sql_data_type(key_field_type, typ)
            else:
                # an embedded user-defined type
                return SqlUserDefinedType(typ.__name__)
        if is_type_enum(typ):
            value_types = enum_value_types(typ)
            if len(value_types) > 1:
                raise TypeError(
                    f"inconsistent enumeration value types for type {typ.__name__}: {value_types}"
                )
            value_type = value_types.pop()

            if self.enum_as_type:
                return SqlUserDefinedType(typ.__name__)
            else:
                return self.member_to_sql_data_type(value_type, cls)
        if isinstance(typ, typing.ForwardRef):
            eval_type = typ._evaluate(
                sys.modules[cls.__module__].__dict__, locals(), frozenset()
            )
            return self.member_to_sql_data_type(eval_type)
        if is_generic_list(typ):
            item_type = unwrap_generic_list(typ)
            if item_type is bool:
                return SqlArrayType(SqlBooleanType())
            if item_type is int:
                return SqlArrayType(SqlIntegerType(8))
            if item_type is float:
                return SqlArrayType(SqlDoubleType())
            if item_type is str:
                return SqlArrayType(SqlCharacterType())
            if is_dataclass_type(item_type) and _dataclass_has_primary_key(item_type):
                raise TypeError(
                    f"use a join table, unable to convert list of entity type with primary key: {typ}"
                )
            if isinstance(item_type, type):
                return SqlArrayType(SqlUserDefinedType(item_type.__name__))
            raise TypeError(f"unsupported array data type: {item_type}")
        if isinstance(typ, type):
            return SqlUserDefinedType(typ.__name__)

        raise TypeError(f"unsupported data type: {typ}")

    def member_to_column(
        self, field: DataclassField, cls: type[DataclassInstance]
    ) -> Column:
        props = get_field_properties(field.type)
        typ = props.field_type

        if is_type_optional(typ):
            typ = unwrap_optional_type(typ)
            nullable = True
        else:
            nullable = False

        data_type = self.member_to_sql_data_type(typ, cls)

        return Column(LocalId(field.name), data_type, nullable)

    def dataclass_to_table(
        self, cls: type[DataclassInstance], *, namespace: Optional[str] = None
    ) -> Table:
        if not is_dataclass_type(cls):
            raise TypeError("expected: dataclass type")

        try:
            columns = [
                self.member_to_column(field, cls)
                for field in dataclass_fields(cls)
                if not is_generic_list(field.type)
            ]
        except TypeError as e:
            raise TypeError(f"error processing data-class: {cls}") from e

        # table constraints
        constraints: list[Constraint] = []

        # foreign keys
        for field in dataclass_fields(cls):
            if is_dataclass_type(field.type):
                constraints.append(
                    ForeignConstraint(
                        LocalId(f"fk_{cls.__name__}_{field.name}"),
                        LocalId(field.name),
                        QualifiedId(namespace, field.type.__name__),
                        _dataclass_primary_key_name(field.type),
                    )
                )

        # checks
        if not self.enum_as_type:
            for field in dataclass_fields(cls):
                if is_type_enum(field.type):
                    enum_values = ", ".join(quote(e.value) for e in field.type)
                    constraints.append(
                        CheckConstraint(
                            LocalId(f"ch_{cls.__name__}_{field.name}"),
                            f"{LocalId(field.name)} IN ({enum_values})",
                        ),
                    )

        return Table(
            QualifiedId(namespace, cls.__name__),
            columns,
            _dataclass_primary_key_name(cls),
            constraints if constraints else None,
        )

    def dataclass_to_struct(
        self, cls: type[DataclassInstance], *, namespace: Optional[str] = None
    ) -> StructType:
        try:
            members = [
                StructMember(
                    LocalId(field.name), self.member_to_sql_data_type(field.type, cls)
                )
                for field in dataclass_fields(cls)
            ]
        except TypeError as e:
            raise TypeError(f"error processing data-class: {cls}") from e

        return StructType(QualifiedId(namespace, cls.__name__), members)


def dataclass_to_table(
    cls: type[DataclassInstance], *, namespace: Optional[str] = None
) -> Table:
    converter = DataclassConverter(enum_as_type=True)
    return converter.dataclass_to_table(cls, namespace=namespace)


def dataclass_to_struct(
    cls: type[DataclassInstance], *, namespace: Optional[str] = None
) -> StructType:
    converter = DataclassConverter(enum_as_type=True)
    return converter.dataclass_to_struct(cls, namespace=namespace)


def _has_user_defined_members(cls: type) -> bool:
    for field in dataclass_fields(cls):
        if not is_simple_type(field.type):
            return True
    return False


def module_to_sql(module: types.ModuleType) -> Namespace:
    converter = DataclassConverter()

    enums = [
        EnumType(obj, namespace=module.__name__)
        for name, obj in inspect.getmembers(module, is_type_enum)
    ]
    items = [obj for name, obj in inspect.getmembers(module, is_struct_type)]
    items.sort(key=lambda obj: _has_user_defined_members(obj))
    structs = [
        converter.dataclass_to_struct(item, namespace=module.__name__) for item in items
    ]
    entities = [obj for name, obj in inspect.getmembers(module, is_entity_type)]
    tables = [
        converter.dataclass_to_table(cls, namespace=module.__name__) for cls in entities
    ]

    # create join tables for one-to-many relationships
    for entity in entities:
        for field in dataclass_fields(entity):
            if not is_generic_list(field.type):
                continue

            item_type = unwrap_generic_list(field.type)
            if not is_entity_type(item_type):
                continue

            table_left = entity.__name__
            table_right = item_type.__name__
            primary_key_left = _dataclass_primary_key_name(entity)
            primary_key_right = _dataclass_primary_key_name(item_type)
            column_left = f"{table_left}_{field.name}"
            column_right = f"{table_right}_{primary_key_right.name}"
            tables.append(
                Table(
                    QualifiedId(module.__name__, f"{column_left}_{table_right}"),
                    [
                        Column(
                            LocalId("uuid"),
                            converter.member_to_sql_data_type(uuid.UUID, entity),
                            False,
                        ),
                        Column(
                            LocalId(column_left),
                            converter.member_to_sql_data_type(
                                _dataclass_primary_key_type(entity), entity
                            ),
                            False,
                        ),
                        Column(
                            LocalId(column_right),
                            converter.member_to_sql_data_type(
                                _dataclass_primary_key_type(item_type), item_type
                            ),
                            False,
                        ),
                    ],
                    LocalId("uuid"),
                    [
                        ForeignConstraint(
                            LocalId(f"jk_{column_left}"),
                            LocalId(column_left),
                            QualifiedId(module.__name__, table_left),
                            primary_key_left,
                        ),
                        ForeignConstraint(
                            LocalId(f"jk_{column_right}"),
                            LocalId(column_right),
                            QualifiedId(module.__name__, table_right),
                            primary_key_right,
                        ),
                    ],
                )
            )

    return Namespace(enums=enums, structs=structs, tables=tables)
