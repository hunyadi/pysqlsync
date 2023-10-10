import dataclasses
import datetime
import decimal
import enum
import ipaddress
import sys
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
from strong_typing.core import JsonType
from strong_typing.docstring import Docstring, parse_type
from strong_typing.inspection import (
    DataclassField,
    DataclassInstance,
    TypeLike,
    dataclass_fields,
    enum_value_types,
    evaluate_member_type,
    get_referenced_types,
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
from strong_typing.topological import type_topological_sort

from ..model.data_types import *
from ..model.id_types import GlobalId, LocalId, PrefixedId, QualifiedId
from ..model.properties import get_field_properties
from .inspection import (
    dataclass_primary_key_name,
    dataclass_primary_key_type,
    get_entity_types,
    is_entity_type,
    is_simple_type,
    is_struct_type,
)
from .object_types import (
    Catalog,
    CheckConstraint,
    Column,
    Constraint,
    ConstraintReference,
    DiscriminatedConstraint,
    EnumType,
    ForeignConstraint,
    Namespace,
    StructMember,
    StructType,
    Table,
    UniqueConstraint,
    constant,
)

T = TypeVar("T")

ENUM_NAME_LENGTH: int = 64
"Maximum length for an enumeration string value."


def is_unique(items: Iterable[T]) -> bool:
    "Uniqueness check of unhashable iterables."

    unique: list[T] = []
    for item in items:
        if item not in unique:
            unique.append(item)
    return len(unique) == 1


def is_relationship(field_type: TypeLike) -> bool:
    if not is_generic_list(field_type):
        return False

    elem_type = unwrap_generic_list(field_type)
    return is_entity_type(elem_type)


def dataclass_fields_as_required(
    cls: type[DataclassInstance],
) -> Iterable[DataclassField]:
    for field in dataclass_fields(cls):
        data_type = (
            unwrap_optional_type(field.type)
            if is_type_optional(field.type)
            else field.type
        )
        ref = evaluate_member_type(data_type, cls)
        yield DataclassField(field.name, ref)


class NamespaceMapping:
    """
    Associates Python modules with SQL namespaces (schemas).

    :param dictionary: Maps a Python module to a SQL namespace, or `None` to use the default namespace.
    """

    dictionary: dict[types.ModuleType, Optional[str]]

    def __init__(
        self, dictionary: Optional[dict[types.ModuleType, Optional[str]]] = None
    ) -> None:
        self.dictionary = dictionary if dictionary is not None else {}

    def get(self, name: str) -> Optional[str]:
        module = sys.modules[name]
        if module in self.dictionary:
            return self.dictionary[module]  # include special return value `None`
        else:
            return module.__name__


@enum.unique
class EnumMode(enum.Enum):
    "Determines whether enumeration types are converted as SQL ENUM, a foreign/primary key relation, or a CHECK constraint."

    TYPE = "type"
    "Enumeration types are converted to SQL ENUM types with CREATE TYPE ... AS ENUM ( ... )."

    INLINE = "inline"
    "Enumeration types are converted into inline SQL ENUM definitions."

    RELATION = "relation"
    "Enumeration types are converted into foreign/primary key relations, and reference a lookup table."

    CHECK = "check"
    "Enumeration types are converted into their value type, with a CHECK constraint on the column."


@enum.unique
class StructMode(enum.Enum):
    "Determines how to convert structure types that are not mapped into SQL tables."

    TYPE = "type"
    "Dataclass types are converted into SQL types with CREATE TYPE ... AS ( ... )."

    JSON = "json"
    "Dataclass types are converted into SQL JSON type (or text)."


@enum.unique
class ArrayMode(enum.Enum):
    ARRAY = "array"
    JSON = "json"


@dataclass
class DataclassConverterOptions:
    """
    Configuration options for generating a SQL table definition from a Python dataclass.

    :param enum_mode: Conversion mode for enumeration types.
    :param struct_mode: Conversion mode for structure types that are not entities.
    :param extra_numeric_types: Whether to use extra numeric types like `int1` or `uint4`.
    :param qualified_names: Whether to use fully qualified names (True) or string-prefixed names (False).
    :param namespaces: Maps Python modules to SQL namespaces (schemas).
    :param foreign_constraints: Whether to create foreign/primary key relationships between tables.
    :param substitutions: SQL type to be substituted for a specific Python type.
    :param column_class: The object type instantiated for table columns. Must derive from `Column`.
    :param table_class: The object type instantiated for tables. Must derive from `Table`.
    :param struct_class: The object type instantiated for struct types. Must derive from `StructType`.
    :param skip_annotations: Annotation classes to ignore on table column types.
    """

    enum_mode: EnumMode = EnumMode.TYPE
    struct_mode: StructMode = StructMode.TYPE
    array_mode: ArrayMode = ArrayMode.ARRAY
    extra_numeric_types: bool = False
    qualified_names: bool = True
    namespaces: NamespaceMapping = dataclasses.field(default_factory=NamespaceMapping)
    foreign_constraints: bool = True
    substitutions: dict[TypeLike, SqlDataType] = dataclasses.field(default_factory=dict)
    column_class: type[Column] = Column
    table_class: type[Table] = Table
    struct_class: type[StructType] = StructType
    skip_annotations: tuple[type, ...] = ()


class DataclassConverter:
    options: DataclassConverterOptions

    def __init__(
        self,
        *,
        options: Optional[DataclassConverterOptions] = None,
    ):
        if options is not None:
            self.options = options
        else:
            self.options = DataclassConverterOptions()

    def create_qualified_id(
        self, module_name: str, object_name: str
    ) -> SupportsQualifiedId:
        mapped_name = self.options.namespaces.get(module_name)
        if self.options.qualified_names:
            return QualifiedId(mapped_name, object_name)
        else:
            return PrefixedId(mapped_name, object_name)

    def simple_type_to_sql_data_type(self, typ: TypeLike) -> SqlDataType:
        if typ is bool:
            return SqlBooleanType()
        if typ is int:
            return SqlIntegerType(8)
        if typ is float:
            return SqlDoubleType()
        if typ is int8:
            if self.options.extra_numeric_types:
                return SqlUserDefinedType(
                    GlobalId("int1")
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
                    GlobalId("uint1")
                )  # PostgreSQL extension required
            else:
                return SqlIntegerType(2)
        if typ is uint16:
            if self.options.extra_numeric_types:
                return SqlUserDefinedType(
                    GlobalId("uint2")
                )  # PostgreSQL extension required
            else:
                return SqlIntegerType(2)
        if typ is uint32:
            if self.options.extra_numeric_types:
                return SqlUserDefinedType(
                    GlobalId("uint4")
                )  # PostgreSQL extension required
            else:
                return SqlIntegerType(4)
        if typ is uint64:
            if self.options.extra_numeric_types:
                return SqlUserDefinedType(
                    GlobalId("uint8")
                )  # PostgreSQL extension required
            else:
                return SqlIntegerType(8)
        if typ is float32:
            return SqlRealType()
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
        if typ is JsonType:
            return SqlJsonType()
        if typ is ipaddress.IPv4Address or typ is ipaddress.IPv6Address:
            return SqlUserDefinedType(GlobalId("inet"))  # PostgreSQL only

        metadata = getattr(typ, "__metadata__", None)
        if metadata:
            unadorned_type = unwrap_annotated_type(typ)
            sql_type: Optional[SqlDataType]
            if unadorned_type is str:
                sql_type = SqlCharacterType()
            elif unadorned_type is decimal.Decimal:
                sql_type = SqlDecimalType()
            elif unadorned_type is datetime.datetime:
                sql_type = SqlTimestampType()
            elif unadorned_type is datetime.time:
                sql_type = SqlTimeType()
            else:
                # avoid error when only skipped annotations are applied to a custom type
                sql_type = None

            for meta in metadata:
                if isinstance(meta, self.options.skip_annotations):
                    continue
                if sql_type is None:
                    raise TypeError(f"unsupported annotated Python type: {typ}")
                sql_type.parse_meta(meta)

            if sql_type:
                return sql_type

            typ = unadorned_type

        if is_type_enum(typ):
            value_types = enum_value_types(typ)
            if len(value_types) > 1:
                raise TypeError(
                    f"inconsistent enumeration value types for type {typ.__name__}: {value_types}"
                )
            value_type = value_types.pop()

            if self.options.enum_mode is EnumMode.TYPE:
                return SqlUserDefinedType(
                    self.create_qualified_id(typ.__module__, typ.__name__)
                )
            elif self.options.enum_mode is EnumMode.INLINE:
                return SqlEnumType([str(e.value) for e in typ])
            elif self.options.enum_mode is EnumMode.RELATION:
                return SqlIntegerType(4)
            elif self.options.enum_mode is EnumMode.CHECK:
                return self.simple_type_to_sql_data_type(value_type)

        raise TypeError(f"not a simple type: {typ}")

    def member_to_sql_data_type(self, typ: TypeLike, cls: type) -> SqlDataType:
        """
        Maps a native Python type into a SQL data type.

        :param typ: The type to convert, typically a dataclass member type.
        :param cls: The context for the type, typically the dataclass in which the member is defined.
        """

        substitute = self.options.substitutions.get(typ)
        if substitute is not None:
            return substitute

        if is_simple_type(typ):
            return self.simple_type_to_sql_data_type(typ)
        if is_entity_type(typ):
            # a many-to-one relationship to another entity class
            key_field_type = dataclass_primary_key_type(typ)
            return self.member_to_sql_data_type(key_field_type, typ)
        if is_dataclass_type(typ):
            # an embedded user-defined type
            if self.options.struct_mode is StructMode.TYPE:
                return SqlUserDefinedType(
                    self.create_qualified_id(typ.__module__, typ.__name__)
                )
            elif self.options.struct_mode is StructMode.JSON:
                return self.member_to_sql_data_type(JsonType, cls)
        if isinstance(typ, typing.ForwardRef):
            return self.member_to_sql_data_type(evaluate_member_type(typ, cls), cls)
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
                if self.options.array_mode is ArrayMode.ARRAY:
                    return SqlArrayType(self.simple_type_to_sql_data_type(item_type))
                elif self.options.array_mode is ArrayMode.JSON:
                    return SqlJsonType()
            if is_entity_type(item_type):
                raise TypeError(
                    f"use a join table, unable to convert list of entity type with primary key: {typ}"
                )
            if isinstance(item_type, type):
                if self.options.array_mode is ArrayMode.ARRAY:
                    return SqlArrayType(
                        SqlUserDefinedType(
                            self.create_qualified_id(
                                item_type.__module__, item_type.__name__
                            )
                        )
                    )
                elif self.options.array_mode is ArrayMode.JSON:
                    return SqlJsonType()

            raise TypeError(f"unsupported array data type: {item_type}")
        if is_type_union(typ):
            member_types = [
                evaluate_member_type(t, cls) for t in unwrap_union_types(typ)
            ]
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
            try:
                return compatible_type(sql_member_types)
            except CompatibilityError:
                raise TypeError(f"inconsistent union data types: {member_types}")
        if isinstance(typ, type):
            return SqlUserDefinedType(
                self.create_qualified_id(typ.__module__, typ.__name__)
            )

        raise TypeError(f"unsupported data type: {typ}")

    def member_to_column(
        self, field: DataclassField, cls: type[DataclassInstance], doc: Docstring
    ) -> Column:
        "Converts a data-class field into a SQL table column."

        props = get_field_properties(field.type)
        data_type = self.member_to_sql_data_type(props.field_type, cls)
        description = (
            doc.params[field.name].description if field.name in doc.params else None
        )

        return self.options.column_class(
            name=LocalId(field.name),
            data_type=data_type,
            nullable=props.nullable,
            description=description,
        )

    def dataclass_to_table(self, cls: type[DataclassInstance]) -> Table:
        "Converts a data-class with a primary key into a SQL table type."

        if not is_dataclass_type(cls):
            raise TypeError(f"expected: dataclass type; got: {cls}")

        doc = parse_type(cls)

        try:
            columns = [
                self.member_to_column(field, cls, doc)
                for field in dataclass_fields(cls)
                if not is_relationship(field.type)
            ]
        except TypeError as e:
            raise TypeError(f"error processing data-class: {cls}") from e

        # foreign/primary key constraints
        constraints = (
            self.dataclass_to_constraints(cls)
            if self.options.foreign_constraints
            else []
        )

        # checks
        if self.options.enum_mode is EnumMode.RELATION:
            for field in dataclass_fields_as_required(cls):
                if is_type_enum(field.type):
                    constraints.append(
                        ForeignConstraint(
                            LocalId(f"fk_{cls.__name__}_{field.name}"),
                            LocalId(field.name),
                            ConstraintReference(
                                self.create_qualified_id(
                                    field.type.__module__, field.type.__name__
                                ),
                                LocalId("id"),
                            ),
                        ),
                    )
        elif self.options.enum_mode is EnumMode.CHECK:
            for field in dataclass_fields_as_required(cls):
                if is_type_enum(field.type):
                    enum_values = ", ".join(constant(e.value) for e in field.type)
                    constraints.append(
                        CheckConstraint(
                            LocalId(f"ch_{cls.__name__}_{field.name}"),
                            f"{LocalId(field.name)} IN ({enum_values})",
                        ),
                    )

        return self.options.table_class(
            name=self.create_qualified_id(cls.__module__, cls.__name__),
            columns=columns,
            primary_key=LocalId(dataclass_primary_key_name(cls)),
            constraints=constraints or None,
            description=doc.full_description,
        )

    def dataclass_to_constraints(
        self, cls: type[DataclassInstance]
    ) -> list[Constraint]:
        "Extracts the foreign/primary key relationships from a data-class."

        constraints: list[Constraint] = []

        for field in dataclass_fields_as_required(cls):
            if is_entity_type(field.type):
                # foreign keys
                constraints.append(
                    ForeignConstraint(
                        LocalId(f"fk_{cls.__name__}_{field.name}"),
                        LocalId(field.name),
                        ConstraintReference(
                            self.create_qualified_id(
                                field.type.__module__, field.type.__name__
                            ),
                            LocalId(dataclass_primary_key_name(field.type)),
                        ),
                    )
                )

            if is_type_union(field.type):
                member_types = [
                    evaluate_member_type(t, cls) for t in unwrap_union_types(field.type)
                ]
                if all(is_entity_type(t) for t in member_types):
                    # discriminated keys
                    constraints.append(
                        DiscriminatedConstraint(
                            LocalId(f"dk_{cls.__name__}_{field.name}"),
                            LocalId(field.name),
                            [
                                ConstraintReference(
                                    self.create_qualified_id(t.__module__, t.__name__),
                                    LocalId(dataclass_primary_key_name(t)),
                                )
                                for t in member_types
                            ],
                        )
                    )

        return constraints

    def member_to_field(
        self, field: DataclassField, cls: type[DataclassInstance], doc: Docstring
    ) -> StructMember:
        "Converts a data-class field into a SQL struct (composite type) field."

        props = get_field_properties(field.type)
        description = (
            doc.params[field.name].description if field.name in doc.params else None
        )

        return StructMember(
            name=LocalId(field.name),
            data_type=self.member_to_sql_data_type(props.field_type, cls),
            description=description,
        )

    def dataclass_to_struct(self, cls: type[DataclassInstance]) -> StructType:
        "Converts a data-class without a primary key into a SQL struct type."

        if not is_dataclass_type(cls):
            raise TypeError(f"expected: dataclass type; got: {cls}")

        doc = parse_type(cls)

        try:
            members = [
                self.member_to_field(field, cls, doc) for field in dataclass_fields(cls)
            ]
        except TypeError as e:
            raise TypeError(f"error processing data-class: {cls}") from e

        return self.options.struct_class(
            name=self.create_qualified_id(cls.__module__, cls.__name__),
            members=members,
            description=doc.full_description,
        )

    def dataclasses_to_catalog(
        self, entity_types: list[type[DataclassInstance]]
    ) -> Catalog:
        "Converts a list of Python data-class types into a database object catalog."

        # collect all dependent types
        referenced_types: set[type] = set(entity_types)
        for entity_type in entity_types:
            for ref_type in get_referenced_types(entity_type):
                referenced_types.add(ref_type)

        # collect all enumeration types in alphabetical order
        enums: dict[str, list[EnumType]] = {}
        if self.options.enum_mode is EnumMode.TYPE:
            enum_types = [obj for obj in referenced_types if is_type_enum(obj)]
            enum_types.sort(key=lambda e: e.__name__)
            for enum_type in enum_types:
                enum_defs = enums.setdefault(enum_type.__module__, [])
                enum_defs.append(
                    EnumType(
                        QualifiedId(
                            self.options.namespaces.get(enum_type.__module__),
                            enum_type.__name__,
                        ),
                        [str(e.value) for e in enum_type],
                    )
                )

        # collect all struct types in alphabetical order
        structs: dict[str, list[StructType]] = {}
        if self.options.struct_mode is StructMode.TYPE:
            struct_types = [obj for obj in referenced_types if is_struct_type(obj)]
            struct_types.sort(key=lambda s: s.__name__)
            depend_types = type_topological_sort(struct_types)
            for depend_type in depend_types:
                if depend_type not in struct_types:
                    continue

                struct_defs = structs.setdefault(depend_type.__module__, [])
                struct_defs.append(self.dataclass_to_struct(depend_type))

        # create tables
        tables: dict[str, list[Table]] = {}
        table_types = [obj for obj in referenced_types if is_entity_type(obj)]
        table_types.sort(key=lambda obj: obj.__name__)
        for table_type in table_types:
            table_defs = tables.setdefault(table_type.__module__, [])
            table_defs.append(self.dataclass_to_table(table_type))

        # create tables for enumerations
        if self.options.enum_mode is EnumMode.RELATION:
            for entity in table_types:
                for field in dataclass_fields_as_required(entity):
                    if not is_type_enum(field.type):
                        continue

                    enum_type = field.type
                    table_defs = tables.setdefault(enum_type.__module__, [])
                    table_defs.append(
                        self.options.table_class(
                            self.create_qualified_id(
                                enum_type.__module__, enum_type.__name__
                            ),
                            [
                                self.options.column_class(
                                    LocalId("id"),
                                    SqlIntegerType(4),
                                    False,
                                    identity=True,
                                ),
                                self.options.column_class(
                                    LocalId("value"),
                                    SqlCharacterType(ENUM_NAME_LENGTH),
                                    False,
                                ),
                            ],
                            primary_key=LocalId("id"),
                            constraints=[
                                UniqueConstraint(
                                    LocalId(f"uq_{enum_type.__name__}"),
                                    LocalId("value"),
                                )
                            ],
                        )
                    )
        # create join tables for one-to-many relationships
        for entity in table_types:
            for field in dataclass_fields(entity):
                if not is_generic_list(field.type):
                    continue

                item_type = unwrap_generic_list(field.type)
                if not is_entity_type(item_type):
                    continue

                primary_key_left = LocalId(dataclass_primary_key_name(entity))
                primary_key_right = LocalId(dataclass_primary_key_name(item_type))
                column_left = f"{entity.__name__}_{field.name}"
                column_right = f"{item_type.__name__}_{primary_key_right.id}"
                table_defs = tables.setdefault(entity.__module__, [])
                table_defs.append(
                    self.options.table_class(
                        self.create_qualified_id(
                            entity.__module__,
                            f"{column_left}_{item_type.__name__}",
                        ),
                        [
                            self.options.column_class(
                                LocalId("uuid"),
                                self.member_to_sql_data_type(uuid.UUID, entity),
                                False,
                            ),
                            self.options.column_class(
                                LocalId(column_left),
                                self.member_to_sql_data_type(
                                    dataclass_primary_key_type(entity), entity
                                ),
                                False,
                            ),
                            self.options.column_class(
                                LocalId(column_right),
                                self.member_to_sql_data_type(
                                    dataclass_primary_key_type(item_type), item_type
                                ),
                                False,
                            ),
                        ],
                        primary_key=LocalId("uuid"),
                        constraints=[
                            ForeignConstraint(
                                LocalId(f"jk_{column_left}"),
                                LocalId(column_left),
                                ConstraintReference(
                                    self.create_qualified_id(
                                        entity.__module__,
                                        entity.__name__,
                                    ),
                                    primary_key_left,
                                ),
                            ),
                            ForeignConstraint(
                                LocalId(f"jk_{column_right}"),
                                LocalId(column_right),
                                ConstraintReference(
                                    self.create_qualified_id(
                                        item_type.__module__,
                                        item_type.__name__,
                                    ),
                                    primary_key_right,
                                ),
                            ),
                        ],
                    )
                )

        if self.options.qualified_names:
            namespaces = [
                Namespace(
                    name=LocalId(self.options.namespaces.get(module_name) or ""),
                    enums=enums.get(module_name, []),
                    structs=structs.get(module_name, []),
                    tables=table_defs,
                )
                for module_name, table_defs in tables.items()
            ]
        else:
            namespaces = [
                Namespace(
                    name=LocalId(""),
                    enums=[item for enum_list in enums.values() for item in enum_list],
                    structs=[
                        item for struct_list in structs.values() for item in struct_list
                    ],
                    tables=[
                        item for table_list in tables.values() for item in table_list
                    ],
                )
            ]
        return Catalog(namespaces=namespaces)

    def modules_to_catalog(self, modules: list[types.ModuleType]) -> Catalog:
        "Converts a list of Python modules into a database object catalog."

        return self.dataclasses_to_catalog(get_entity_types(modules))


def dataclass_to_table(
    cls: type[DataclassInstance],
    *,
    options: Optional[DataclassConverterOptions] = None,
) -> Table:
    "Converts a data-class with a primary key into a SQL table type."

    if options is None:
        options = DataclassConverterOptions()

    converter = DataclassConverter(options=options)
    return converter.dataclass_to_table(cls)


def dataclass_to_struct(
    cls: type[DataclassInstance],
    *,
    options: Optional[DataclassConverterOptions] = None,
) -> StructType:
    "Converts a data-class without a primary key into a SQL struct type."

    if options is None:
        options = DataclassConverterOptions()

    converter = DataclassConverter(options=options)
    return converter.dataclass_to_struct(cls)


def module_to_catalog(
    module: types.ModuleType,
    *,
    options: Optional[DataclassConverterOptions] = None,
) -> Catalog:
    return modules_to_catalog([module], options=options)


def modules_to_catalog(
    modules: list[types.ModuleType],
    *,
    options: Optional[DataclassConverterOptions] = None,
) -> Catalog:
    "Converts the entire contents of a Python module into a SQL namespace (schema)."

    if options is None:
        options = DataclassConverterOptions()

    converter = DataclassConverter(options=options)
    return converter.modules_to_catalog(modules)
