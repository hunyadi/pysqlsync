import datetime
import decimal
import inspect
import ipaddress
import types
import typing
import uuid
from typing import Iterable, Optional, TypeVar

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
from strong_typing.docstring import Docstring, parse_type
from strong_typing.inspection import (
    DataclassField,
    DataclassInstance,
    dataclass_fields,
    enum_value_types,
    evaluate_type,
    is_dataclass_type,
    is_generic_list,
    is_type_enum,
    is_type_literal,
    is_type_optional,
    is_type_union,
    unwrap_annotated_type,
    unwrap_generic_list,
    unwrap_literal_types,
    unwrap_optional_type,
    unwrap_union_types,
)

from ..model.data_types import *
from ..model.properties import get_field_properties
from .inspection import (
    dataclass_primary_key_name,
    dataclass_primary_key_type,
    is_entity_type,
    is_simple_type,
    is_struct_type,
)
from .object_types import (
    CheckConstraint,
    Column,
    Constraint,
    ConstraintReference,
    DiscriminatedConstraint,
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

T = TypeVar("T")


def is_unique(items: Iterable[T]) -> bool:
    "Uniqueness check of unhashable iterables."

    unique: list[T] = []
    for item in items:
        if item not in unique:
            unique.append(item)
    return len(unique) == 1


@dataclass
class DataclassConverterOptions:
    enum_as_type: bool = True
    extra_numeric_types: bool = False
    user_defined_annotation_classes: tuple[type, ...] = ()


class DataclassConverter:
    namespace: Optional[str]
    options: DataclassConverterOptions

    def __init__(
        self,
        *,
        namespace: Optional[str] = None,
        options: Optional[DataclassConverterOptions] = None,
    ):
        self.namespace = namespace
        if options is not None:
            self.options = options
        else:
            self.options = DataclassConverterOptions()

    def simple_type_to_sql_data_type(self, typ: type) -> SqlDataType:
        if typ is bool:
            return SqlBooleanType()
        if typ is int:
            return SqlIntegerType(8)
        if typ is float:
            return SqlDoubleType()
        if typ is int8:
            if self.options.extra_numeric_types:
                return SqlUserDefinedType(
                    QualifiedId(None, "int1")
                )  # PostgreSQL extension required
            else:
                return SqlIntegerType(2)
        if typ is int16:
            return SqlIntegerType(2)
        if typ is int32:
            return SqlIntegerType(4)
        if typ is int64:
            return SqlIntegerType(8)
        if typ is uint8:
            if self.options.extra_numeric_types:
                return SqlUserDefinedType(
                    QualifiedId(None, "uint1")
                )  # PostgreSQL extension required
            else:
                return SqlIntegerType(2)
        if typ is uint16:
            if self.options.extra_numeric_types:
                return SqlUserDefinedType(
                    QualifiedId(None, "uint2")
                )  # PostgreSQL extension required
            else:
                return SqlIntegerType(2)
        if typ is uint32:
            if self.options.extra_numeric_types:
                return SqlUserDefinedType(
                    QualifiedId(None, "uint4")
                )  # PostgreSQL extension required
            else:
                return SqlIntegerType(4)
        if typ is uint64:
            if self.options.extra_numeric_types:
                return SqlUserDefinedType(
                    QualifiedId(None, "uint8")
                )  # PostgreSQL extension required
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
            return SqlUserDefinedType(QualifiedId(None, "inet"))  # PostgreSQL only

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
                if not isinstance(meta, self.options.user_defined_annotation_classes):
                    sql_type.parse_meta(meta)

            return sql_type

        if is_type_enum(typ):
            value_types = enum_value_types(typ)
            if len(value_types) > 1:
                raise TypeError(
                    f"inconsistent enumeration value types for type {typ.__name__}: {value_types}"
                )
            value_type = value_types.pop()

            if self.options.enum_as_type:
                return SqlUserDefinedType(QualifiedId(self.namespace, typ.__name__))
            else:
                return self.simple_type_to_sql_data_type(value_type)

        raise TypeError(f"not a simple type: {typ}")

    def member_to_sql_data_type(self, typ: type, cls: type) -> SqlDataType:
        """
        Maps a native Python type into a SQL data type.

        :param typ: The type to convert, typically a dataclass member type.
        :param cls: The context for the type, typically the dataclass in which the member is defined.
        """

        if is_simple_type(typ):
            return self.simple_type_to_sql_data_type(typ)
        if is_entity_type(typ):
            # a many-to-one relationship to another entity class
            key_field_type = dataclass_primary_key_type(typ)
            return self.member_to_sql_data_type(key_field_type, typ)
        if is_dataclass_type(typ):
            # an embedded user-defined type
            return SqlUserDefinedType(QualifiedId(self.namespace, typ.__name__))
        if isinstance(typ, typing.ForwardRef):
            return self.member_to_sql_data_type(evaluate_type(typ, cls), cls)
        if is_type_literal(typ):
            literal_types = unwrap_literal_types(typ)
            sql_literal_types = [
                self.member_to_sql_data_type(t, cls) for t in literal_types
            ]
            if not is_unique(sql_literal_types):
                raise TypeError(f"inconsistent literal data types: {literal_types}")
            return sql_literal_types[0]
        if is_generic_list(typ):
            item_type = unwrap_generic_list(typ)
            if is_simple_type(item_type):
                return SqlArrayType(self.simple_type_to_sql_data_type(item_type))
            if is_entity_type(item_type):
                raise TypeError(
                    f"use a join table, unable to convert list of entity type with primary key: {typ}"
                )
            if isinstance(item_type, type):
                return SqlArrayType(
                    SqlUserDefinedType(QualifiedId(self.namespace, item_type.__name__))
                )
            raise TypeError(f"unsupported array data type: {item_type}")
        if is_type_union(typ):
            member_types = [evaluate_type(t, cls) for t in unwrap_union_types(typ)]
            if all(is_entity_type(t) for t in member_types):
                # discriminated union type
                primary_key_types = set(
                    dataclass_primary_key_type(t) for t in member_types
                )
                if len(primary_key_types) > 1:
                    raise TypeError(
                        f"mismatching key types in discriminated union of: {member_types}"
                    )
                common_key_type = primary_key_types.pop()
                return self.member_to_sql_data_type(common_key_type, cls)

            sql_member_types = [
                self.member_to_sql_data_type(t, cls) for t in member_types
            ]
            if not is_unique(sql_member_types):
                raise TypeError(f"inconsistent union data types: {member_types}")
            return sql_member_types[0]
        if isinstance(typ, type):
            return SqlUserDefinedType(QualifiedId(self.namespace, typ.__name__))

        raise TypeError(f"unsupported data type: {typ}")

    def member_to_column(
        self, field: DataclassField, cls: type[DataclassInstance], doc: Docstring
    ) -> Column:
        "Converts a data-class field into a SQL table column."

        props = get_field_properties(field.type)
        typ = props.field_type

        if is_type_optional(typ):
            typ = unwrap_optional_type(typ)
            nullable = True
        else:
            nullable = False

        data_type = self.member_to_sql_data_type(typ, cls)

        description = (
            doc.params[field.name].description if field.name in doc.params else None
        )

        return Column(
            name=LocalId(field.name),
            data_type=data_type,
            nullable=nullable,
            description=description,
        )

    def dataclass_to_table(self, cls: type[DataclassInstance]) -> Table:
        "Converts a data-class with a primary key into a SQL table type."

        if not is_dataclass_type(cls):
            raise TypeError("expected: dataclass type")

        doc = parse_type(cls)

        try:
            columns = [
                self.member_to_column(field, cls, doc)
                for field in dataclass_fields(cls)
                if not is_generic_list(field.type)
            ]
        except TypeError as e:
            raise TypeError(f"error processing data-class: {cls}") from e

        # table constraints
        constraints: list[Constraint] = []

        for field in dataclass_fields(cls):
            if is_entity_type(field.type):
                # foreign keys
                constraints.append(
                    ForeignConstraint(
                        LocalId(f"fk_{cls.__name__}_{field.name}"),
                        LocalId(field.name),
                        ConstraintReference(
                            QualifiedId(self.namespace, field.type.__name__),
                            LocalId(dataclass_primary_key_name(field.type)),
                        ),
                    )
                )

            if is_type_union(field.type):
                member_types = [
                    evaluate_type(t, cls) for t in unwrap_union_types(field.type)
                ]
                if all(is_entity_type(t) for t in member_types):
                    # discriminated keys
                    constraints.append(
                        DiscriminatedConstraint(
                            LocalId(f"dk_{cls.__name__}_{field.name}"),
                            LocalId(field.name),
                            [
                                ConstraintReference(
                                    QualifiedId(self.namespace, t.__name__),
                                    LocalId(dataclass_primary_key_name(t)),
                                )
                                for t in member_types
                            ],
                        )
                    )

        # checks
        if not self.options.enum_as_type:
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
            name=QualifiedId(self.namespace, cls.__name__),
            columns=columns,
            primary_key=LocalId(dataclass_primary_key_name(cls)),
            constraints=constraints if constraints else None,
            description=doc.full_description,
        )

    def member_to_field(
        self, field: DataclassField, cls: type[DataclassInstance], doc: Docstring
    ) -> StructMember:
        "Converts a data-class field into a SQL struct (composite type) field."

        props = get_field_properties(field.type)
        typ = props.field_type

        if is_type_optional(typ):
            typ = unwrap_optional_type(typ)

        description = (
            doc.params[field.name].description if field.name in doc.params else None
        )

        return StructMember(
            name=LocalId(field.name),
            data_type=self.member_to_sql_data_type(typ, cls),
            description=description,
        )

    def dataclass_to_struct(self, cls: type[DataclassInstance]) -> StructType:
        "Converts a data-class without a primary key into a SQL struct type."

        if not is_dataclass_type(cls):
            raise TypeError("expected: dataclass type")

        doc = parse_type(cls)

        try:
            members = [
                self.member_to_field(field, cls, doc) for field in dataclass_fields(cls)
            ]
        except TypeError as e:
            raise TypeError(f"error processing data-class: {cls}") from e

        return StructType(
            name=QualifiedId(self.namespace, cls.__name__),
            members=members,
            description=doc.full_description,
        )


def dataclass_to_table(
    cls: type[DataclassInstance],
    *,
    namespace: Optional[str] = None,
    options: Optional[DataclassConverterOptions] = None,
) -> Table:
    "Converts a data-class with a primary key into a SQL table type."

    if options is None:
        options = DataclassConverterOptions(enum_as_type=True)

    converter = DataclassConverter(namespace=namespace, options=options)
    return converter.dataclass_to_table(cls)


def dataclass_to_struct(
    cls: type[DataclassInstance],
    *,
    namespace: Optional[str] = None,
    options: Optional[DataclassConverterOptions] = None,
) -> StructType:
    "Converts a data-class without a primary key into a SQL struct type."

    if options is None:
        options = DataclassConverterOptions(enum_as_type=True)

    converter = DataclassConverter(namespace=namespace, options=options)
    return converter.dataclass_to_struct(cls)


def _has_user_defined_members(cls: type) -> bool:
    for field in dataclass_fields(cls):
        if not is_simple_type(field.type):
            return True
    return False


def module_to_sql(
    module: types.ModuleType,
    *,
    options: Optional[DataclassConverterOptions] = None,
) -> Namespace:
    "Converts the entire contents of a Python module into a SQL namespace (schema)."

    if options is None:
        options = DataclassConverterOptions(enum_as_type=True)
    converter = DataclassConverter(namespace=module.__name__, options=options)

    enums = [
        EnumType(obj, namespace=module.__name__)
        for name, obj in inspect.getmembers(module, is_type_enum)
    ]
    items = [obj for name, obj in inspect.getmembers(module, is_struct_type)]
    items.sort(key=lambda obj: _has_user_defined_members(obj))
    structs = [converter.dataclass_to_struct(item) for item in items]
    entities = [obj for name, obj in inspect.getmembers(module, is_entity_type)]
    tables = [converter.dataclass_to_table(cls) for cls in entities]

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
            primary_key_left = LocalId(dataclass_primary_key_name(entity))
            primary_key_right = LocalId(dataclass_primary_key_name(item_type))
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
                                dataclass_primary_key_type(entity), entity
                            ),
                            False,
                        ),
                        Column(
                            LocalId(column_right),
                            converter.member_to_sql_data_type(
                                dataclass_primary_key_type(item_type), item_type
                            ),
                            False,
                        ),
                    ],
                    LocalId("uuid"),
                    [
                        ForeignConstraint(
                            LocalId(f"jk_{column_left}"),
                            LocalId(column_left),
                            ConstraintReference(
                                QualifiedId(module.__name__, table_left),
                                primary_key_left,
                            ),
                        ),
                        ForeignConstraint(
                            LocalId(f"jk_{column_right}"),
                            LocalId(column_right),
                            ConstraintReference(
                                QualifiedId(module.__name__, table_right),
                                primary_key_right,
                            ),
                        ),
                    ],
                )
            )

    return Namespace(
        name=LocalId(module.__name__), enums=enums, structs=structs, tables=tables
    )
