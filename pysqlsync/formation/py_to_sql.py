import copy
import dataclasses
import datetime
import decimal
import enum
import inspect
import ipaddress
import sys
import types
import typing
import uuid
from typing import Annotated, Any, Callable, Iterable, Optional, TypeVar

from strong_typing.auxiliary import (
    MaxLength,
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
    is_generic_set,
    is_type_enum,
    is_type_literal,
    is_type_optional,
    is_type_union,
    unwrap_annotated_type,
    unwrap_generic_list,
    unwrap_generic_set,
    unwrap_literal_types,
    unwrap_literal_values,
    unwrap_optional_type,
    unwrap_union_types,
)
from strong_typing.topological import type_topological_sort

from ..model.data_types import (
    CompatibilityError,
    SqlArrayType,
    SqlBooleanType,
    SqlDataType,
    SqlDateType,
    SqlDecimalType,
    SqlDoubleType,
    SqlEnumType,
    SqlFixedCharacterType,
    SqlFloatType,
    SqlIntegerType,
    SqlIntervalType,
    SqlJsonType,
    SqlRealType,
    SqlStructMember,
    SqlStructType,
    SqlTimestampType,
    SqlTimeType,
    SqlUserDefinedType,
    SqlUuidType,
    SqlVariableCharacterType,
    compatible_type,
    constant,
)
from ..model.id_types import (
    GlobalId,
    LocalId,
    PrefixedId,
    QualifiedId,
    SupportsQualifiedId,
)
from ..model.properties import get_field_properties
from .inspection import (
    dataclass_primary_key_name,
    dataclass_primary_key_type,
    get_entity_types,
    is_entity_type,
    is_reference_type,
    is_simple_type,
    is_struct_type,
    reference_to_key,
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
    ObjectFactory,
    StructMember,
    StructType,
    Table,
    UniqueConstraint,
)

T = TypeVar("T")

ENUM_NAME_LENGTH: int = 64
"Maximum length for an enumeration string value."

ENUM_LABEL_TYPE = Annotated[str, MaxLength(ENUM_NAME_LENGTH)]


def enum_value_type(enum_type: type[enum.Enum]) -> type:
    value_types = enum_value_types(enum_type)
    if len(value_types) > 1:
        raise TypeError(
            f"inconsistent enumeration value types for type {enum_type.__name__}: {value_types}"
        )
    value_type = value_types.pop()
    return value_type if value_type is not str else ENUM_LABEL_TYPE


def is_type_enum_list(typ: TypeLike) -> bool:
    if not is_generic_list(typ):
        return False

    elem_type = unwrap_generic_list(typ)
    return is_type_enum(elem_type)


def is_type_enum_set(typ: TypeLike) -> bool:
    if not is_generic_set(typ):
        return False

    elem_type = unwrap_generic_set(typ)
    return is_type_enum(elem_type)


def is_extensible_enum_type(typ: TypeLike, cls: type) -> bool:
    """
    True if the type represents an extensible enumeration type.

    :param typ: A type to check.
    :param cls: Context in which to evaluate the type (e.g. a class).
    """

    if not is_type_union(typ):
        return False
    typ = unwrap_annotated_type(typ)
    member_types = [evaluate_member_type(t, cls) for t in unwrap_union_types(typ)]
    for member_type in member_types:
        if member_type is None:
            continue

        member_type = unwrap_annotated_type(member_type)
        if member_type is not str and not is_type_enum(member_type):
            return False
    return True


def unwrap_extensible_enum_type(typ: TypeLike, cls: type) -> type[enum.Enum]:
    if is_type_union(typ):
        member_types = [evaluate_member_type(t, cls) for t in unwrap_union_types(typ)]
        for member_type in member_types:
            if is_type_enum(member_type):
                return member_type
    raise TypeError(
        "extensible enum types are of the form `E | str` where `E` is a subclass of `enum.Enum`"
    )


def dataclass_fields_as_required(
    cls: type[DataclassInstance], pred: Optional[Callable[[TypeLike], bool]] = None
) -> Iterable[DataclassField]:
    for field in dataclass_fields(cls):
        props = get_field_properties(field.type)
        ref = evaluate_member_type(props.field_type, cls)
        if pred is None or pred(ref):
            yield DataclassField(field.name, ref, field.default)


def _topological_sort(class_types: list[type]) -> list[type]:
    """
    Returns a list of types in topological order.

    Types that don't depend on other types (i.e. fundamental types) are first. Types on which no other types depend are last.

    :param types: A list of types (simple or composite).
    :returns: A list of the same types but in topological order.
    """

    return [t for t in type_topological_sort(class_types) if t in class_types]


@dataclasses.dataclass
class DataclassEnumField:
    name: str
    type: type[enum.Enum]


class NamespaceMapping:
    """
    Associates Python modules with SQL namespaces (schemas).

    :param dictionary: Maps a Python module to a SQL namespace, or `None` to use the default namespace.
    """

    dictionary: dict[types.ModuleType, Optional[str]]

    def __init__(
        self, dictionary: Optional[dict[types.ModuleType, Optional[str]]] = None
    ) -> None:
        if dictionary is not None:
            if len(set(dictionary.values())) != len(dictionary):
                raise ValueError(
                    "expected: a one-to-one mapping between Python modules and database schema names"
                )
            self.dictionary = dictionary
        else:
            self.dictionary = {}

    def get(self, name: str) -> Optional[str]:
        module = sys.modules[name]
        if module in self.dictionary:
            return self.dictionary[module]  # include special return value `None`
        else:
            raise ValueError(
                f"expected: database schema name mapping for module: {module.__name__}"
            )


@enum.unique
class EnumMode(enum.Enum):
    "Determines whether enumeration types are converted as SQL ENUM, a foreign/primary key relation, or a CHECK constraint."

    TYPE = "type"
    "Enumeration types are converted to SQL ENUM types with CREATE TYPE ... AS ENUM ( ... )."

    INLINE = "inline"
    "Enumeration types are converted into inline SQL ENUM definitions."

    RELATION = "relation"
    "Enumeration types are converted into foreign/primary key relations (reference constraint) with a lookup table."

    CHECK = "check"
    "Enumeration types are converted into their value type, with a possible CHECK constraint on the column."


@enum.unique
class StructMode(enum.Enum):
    "Determines how to convert structure types that are not mapped into SQL tables."

    TYPE = "type"
    "Dataclass types are converted into SQL types with CREATE TYPE ... AS ( ... )."

    INLINE = "inline"
    "Dataclass types are converted into embedded nested type with STRUCT < ... >."

    JSON = "json"
    "Dataclass types are converted into SQL JSON type (or text)."


@enum.unique
class ArrayMode(enum.Enum):
    "Determines how to treat sequence types."

    ARRAY = "array"
    "Convert Python list type as SQL array type."

    JSON = "json"
    "Convert Python list type to SQL JSON type (or its representation type)."

    RELATION = "relation"
    "Flatten Python list type to a separate table."


@dataclasses.dataclass
class DataclassConverterOptions:
    """
    Configuration options for generating a SQL table definition from a Python dataclass.

    :param enum_mode: Conversion mode for enumeration types.
    :param struct_mode: Conversion mode for structure types that are not entities.
    :param array_mode: Conversion mode for array types.
    :param extra_numeric_types: Whether to use extra numeric types like `int1` or `uint4`.
    :param qualified_names: Whether to use fully qualified names (True) or string-prefixed names (False).
    :param unique_constraint_names: Whether to generate constraint names that are globally unique.
    :param namespaces: Maps Python modules to SQL namespaces (schemas).
    :param foreign_constraints: Whether to create foreign/primary key relationships between tables.
    :param check_constraints: Whether to create check constraints for table columns with a restricted set of values.
    :param initialize_tables: Whether to populate special tables (e.g. enumerations) with data.
    :param substitutions: SQL type to be substituted for a specific Python type.
    :param factory: Creates new column, table, struct and namespace instances.
    :param skip_annotations: Annotation classes to ignore on table column types.
    :param auto_default: Automatically assign a default value to non-nullable types.
    """

    enum_mode: EnumMode = EnumMode.TYPE
    struct_mode: StructMode = StructMode.TYPE
    array_mode: ArrayMode = ArrayMode.ARRAY
    extra_numeric_types: bool = False
    qualified_names: bool = True
    unique_constraint_names: bool = True
    namespaces: NamespaceMapping = dataclasses.field(default_factory=NamespaceMapping)
    foreign_constraints: bool = True
    check_constraints: bool = True
    initialize_tables: bool = False
    substitutions: dict[TypeLike, SqlDataType] = dataclasses.field(default_factory=dict)
    factory: ObjectFactory = dataclasses.field(default_factory=ObjectFactory)
    skip_annotations: tuple[type, ...] = ()
    auto_default: bool = False


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

    def create_qualified_prefix(self, cls: type) -> str:
        if self.options.unique_constraint_names:
            id = self.create_qualified_id(cls.__module__, cls.__name__)
            return id.compact_id.replace(".", "_")
        else:
            return cls.__name__

    def create_foreign_key_name(self, cls: type, field: str) -> str:
        return f"fk_{self.create_qualified_prefix(cls)}_{field}"

    def create_discriminated_key_name(self, cls: type, field: str) -> str:
        return f"dk_{self.create_qualified_prefix(cls)}_{field}"

    def create_unique_key_name(self, cls: type, field: str) -> str:
        return f"uq_{self.create_qualified_prefix(cls)}_{field}"

    def create_check_name(self, cls: type, field: str) -> str:
        return f"ch_{self.create_qualified_prefix(cls)}_{field}"

    def simple_type_to_sql_data_type(self, typ: TypeLike) -> SqlDataType:
        substitute = self.options.substitutions.get(typ)
        if substitute is not None:
            return substitute

        if typ is bool:
            return SqlBooleanType()
        if typ is int:
            return SqlIntegerType(8)
        if typ is float:
            return SqlDoubleType()
        if typ is int8:
            if self.options.extra_numeric_types:
                return SqlUserDefinedType(GlobalId("int1"))
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
                return SqlUserDefinedType(GlobalId("uint1"))
            else:
                return SqlIntegerType(2)
        if typ is uint16:
            if self.options.extra_numeric_types:
                return SqlUserDefinedType(GlobalId("uint2"))
            else:
                return SqlIntegerType(2)
        if typ is uint32:
            if self.options.extra_numeric_types:
                return SqlUserDefinedType(GlobalId("uint4"))
            else:
                return SqlIntegerType(4)
        if typ is uint64:
            if self.options.extra_numeric_types:
                return SqlUserDefinedType(GlobalId("uint8"))
            else:
                return SqlIntegerType(8)
        if typ is float32:
            return SqlRealType()
        if typ is float64:
            return SqlDoubleType()
        if typ is str:
            return SqlVariableCharacterType()
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
            substitute = self.options.substitutions.get(unadorned_type)
            if substitute is not None:
                sql_type = copy.copy(substitute)
            elif unadorned_type is int:
                sql_type = SqlIntegerType(8)
            elif unadorned_type is str:
                sql_type = SqlVariableCharacterType()
            elif unadorned_type is float:
                sql_type = SqlFloatType()
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
            value_type = enum_value_type(typ)
            unadorned_value_type = unwrap_annotated_type(value_type)

            if unadorned_value_type is int or unadorned_value_type is str:
                if self.options.enum_mode is EnumMode.TYPE:
                    return SqlUserDefinedType(
                        self.create_qualified_id(typ.__module__, typ.__name__)
                    )
                elif self.options.enum_mode is EnumMode.INLINE:
                    return SqlEnumType([str(e.value) for e in typ])
                elif self.options.enum_mode is EnumMode.RELATION:
                    return self._enumeration_key_type()
                elif self.options.enum_mode is EnumMode.CHECK:
                    return self.simple_type_to_sql_data_type(value_type)
                else:
                    raise NotImplementedError(f"match not exhaustive: {EnumMode}")
            else:
                if self.options.enum_mode is EnumMode.RELATION:
                    return self._enumeration_key_type()
                else:
                    raise TypeError(
                        f"enumeration mode {self.options.enum_mode} not valid for complex member types in enumeration `{typ.__name__}`"
                    )

        raise TypeError(f"not a simple type: {typ}")

    def _enumeration_key_type(self) -> SqlDataType:
        "Key type for storing enumeration values in a dedicated table."

        return self.simple_type_to_sql_data_type(int32)

    def _get_relationship(
        self, field_type: TypeLike
    ) -> Optional[tuple[type, TypeLike]]:
        """
        Returns relationship type and field type if the field expands into a separate relation (join table).

        Join tables hold many-to-many relationships. For example, assume a table `Student` has a field `guardians`
        of type `list[Person]`. This would expand into a separate table where each record holds:

        1. a unique identifier
        2. a foreign key to the originating table `Student`
        3. a foreign key to the table `Person`

        Likewise, a field of type `list[enum.Enum]` with options `ArrayMode.RELATION` and `EnumMode.RELATION` would
        expand to a table of

        1. a unique identifier
        2. a foreign key to the originating table
        3. a foreign key to the lookup table with all possible enumeration values

        :param field_type: A type to check.
        :returns: A tuple of host relationship type (e.g. list) and host field type.
        """

        if is_type_optional(field_type):
            field_type = unwrap_optional_type(field_type)

        field_type = unwrap_annotated_type(field_type)

        # relations must be one-to-many
        relation_type: type
        elem_type: TypeLike
        if is_generic_list(field_type):
            relation_type = list
            elem_type = unwrap_generic_list(field_type)
        elif is_generic_set(field_type):
            relation_type = set
            elem_type = unwrap_generic_set(field_type)
        else:
            return None

        if self.options.array_mode is ArrayMode.RELATION or is_entity_type(elem_type):
            return relation_type, elem_type
        else:
            return None

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
            elif self.options.struct_mode is StructMode.INLINE:
                inline_types: list[SqlStructMember] = []
                for field in dataclass_fields(unwrap_annotated_type(typ)):
                    inline_props = get_field_properties(field.type)
                    inline_type = self.member_to_sql_data_type(
                        inline_props.field_type, typ
                    )
                    inline_types.append(
                        SqlStructMember(
                            LocalId(field.name), inline_type, inline_props.nullable
                        )
                    )
                return SqlStructType(inline_types)
            elif self.options.struct_mode is StructMode.JSON:
                return self.member_to_sql_data_type(JsonType, cls)
        if isinstance(typ, typing.ForwardRef):
            return self.member_to_sql_data_type(evaluate_member_type(typ, cls), cls)
        if is_type_literal(typ):
            literal_types = unwrap_literal_types(typ)
            if not all(t is str for t in literal_types):
                sql_literal_types = [
                    self.member_to_sql_data_type(t, cls) for t in literal_types
                ]
            else:
                literal_values = unwrap_literal_values(typ)
                sql_literal_types = [
                    SqlFixedCharacterType(limit=len(v)) for v in literal_values
                ]
            return compatible_type(sql_literal_types)
        if is_generic_list(typ) or is_generic_set(typ):
            if is_generic_set(typ):
                item_type = unwrap_generic_set(typ)
            elif is_generic_list(typ):
                item_type = unwrap_generic_list(typ)
            else:
                raise NotImplementedError("match not exhaustive: list | set")
            if is_simple_type(item_type):
                if self.options.array_mode is ArrayMode.ARRAY:
                    return SqlArrayType(self.simple_type_to_sql_data_type(item_type))
                elif self.options.array_mode is ArrayMode.JSON:
                    return self.member_to_sql_data_type(JsonType, cls)
                elif self.options.array_mode is ArrayMode.RELATION:
                    raise TypeError(
                        "use a join table for flattening a list of values into a separate table"
                    )
            if is_entity_type(item_type):
                # list of entity type (i.e. type with primary key) cannot become SQL array (only list of struct type can)
                raise TypeError(
                    f"use a join table; cannot convert list of entity type to SQL array: {typ}"
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
                    return self.member_to_sql_data_type(JsonType, cls)
                elif self.options.array_mode is ArrayMode.RELATION:
                    raise TypeError(
                        "use a join table for flattening a list of struct type into a separate table"
                    )

            raise TypeError(f"unsupported array data type: {item_type}")
        if is_extensible_enum_type(typ, cls):
            return self._enumeration_key_type()
        if is_type_union(typ):
            member_types = [
                evaluate_member_type(t, cls)
                for t in unwrap_union_types(unwrap_annotated_type(typ))
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

        if field.default is not dataclasses.MISSING and field.default is not None:
            default_value = constant(field.default)
        elif (
            self.options.auto_default
            and not props.nullable
            and not props.is_primary
            and not props.is_identity
        ):
            if is_reference_type(props.plain_type, cls):
                plain_type = reference_to_key(props.plain_type, cls)
            elif is_type_union(props.plain_type):
                member_types = [
                    evaluate_member_type(t, cls)
                    for t in unwrap_union_types(props.plain_type)
                ]
                for member_type in member_types:
                    if is_simple_type(member_type):
                        plain_type = member_type
                        break
                else:
                    plain_type = None
            elif (
                not is_struct_type(props.plain_type)
                and not is_generic_list(props.plain_type)
                and not is_generic_set(props.plain_type)
                and not is_type_union(props.plain_type)
            ):
                plain_type = props.plain_type
            else:
                # unable to infer default value for type
                plain_type = None

            if plain_type is not None:
                default_expr: Any
                if plain_type is datetime.datetime:
                    default_expr = datetime.datetime.min
                elif plain_type is datetime.date:
                    default_expr = datetime.date.min
                elif plain_type is datetime.time:
                    default_expr = datetime.time.min
                elif is_type_enum(plain_type):
                    default_expr = next(member.value for member in plain_type)
                else:
                    default_expr = plain_type()  # type: ignore
                default_value = constant(default_expr)
            else:
                default_value = None
        else:
            default_value = None

        description = (
            doc.params[field.name].description if field.name in doc.params else None
        )

        return self.options.factory.column_class(
            name=LocalId(field.name),
            data_type=data_type,
            nullable=props.nullable,
            default=default_value,
            identity=props.is_identity,
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
                if self._get_relationship(field.type) is None
            ]
        except TypeError as e:
            raise TypeError(f"error processing data-class: {cls}") from e

        # foreign/primary key constraints
        constraints = []
        if self.options.foreign_constraints:
            constraints.extend(self.dataclass_to_constraints(cls))

        # relationships for extensible enumeration types ignore foreign constraints option and always create a foreign key
        for enum_field in dataclass_fields_as_required(
            cls, lambda t: is_extensible_enum_type(t, cls)
        ):
            enum_type = unwrap_extensible_enum_type(enum_field.type, cls)
            constraints.append(
                ForeignConstraint(
                    LocalId(self.create_foreign_key_name(cls, enum_field.name)),
                    (LocalId(enum_field.name),),
                    ConstraintReference(
                        self.create_qualified_id(
                            enum_type.__module__, enum_type.__name__
                        ),
                        (LocalId("id"),),
                    ),
                ),
            )

        # relationships for enumeration types ignore foreign constraints option and always create a foreign key
        if self.options.enum_mode is EnumMode.RELATION:
            for enum_field in dataclass_fields_as_required(cls, is_type_enum):
                constraints.append(
                    ForeignConstraint(
                        LocalId(self.create_foreign_key_name(cls, enum_field.name)),
                        (LocalId(enum_field.name),),
                        ConstraintReference(
                            self.create_qualified_id(
                                enum_field.type.__module__, enum_field.type.__name__
                            ),
                            (LocalId("id"),),
                        ),
                    ),
                )

        if self.options.check_constraints and self.options.enum_mode is EnumMode.CHECK:
            for enum_field in dataclass_fields_as_required(cls, is_type_enum):
                enum_values = ", ".join(constant(e.value) for e in enum_field.type)
                constraints.append(
                    CheckConstraint(
                        LocalId(self.create_check_name(cls, enum_field.name)),
                        f"{LocalId(enum_field.name)} IN ({enum_values})",
                    ),
                )

        for column in columns:
            check_constraint = self.create_check_constraint(column)
            if check_constraint:
                constraints.append(
                    CheckConstraint(
                        LocalId(self.create_check_name(cls, column.name.id)),
                        check_constraint,
                    ),
                )

        return self.options.factory.table_class(
            name=self.create_qualified_id(cls.__module__, cls.__name__),
            columns=columns,
            primary_key=(LocalId(dataclass_primary_key_name(cls)),),
            constraints=constraints or None,
            description=doc.full_description,
        )

    def create_check_constraint(self, column: Column) -> Optional[str]:
        conditions: list[str] = []
        data_type = column.data_type
        if isinstance(data_type, SqlIntegerType):
            if data_type.minimum is not None:
                conditions.append(f"{column.name} >= {data_type.minimum}")
            if data_type.maximum is not None:
                conditions.append(f"{column.name} <= {data_type.maximum}")
        if conditions:
            return " AND ".join(f"({c})" for c in conditions)
        else:
            return None

    def dataclass_to_constraints(
        self, cls: type[DataclassInstance]
    ) -> list[Constraint]:
        "Extracts the foreign/primary key relationships from a data-class."

        constraints: list[Constraint] = []

        for field in dataclass_fields(cls):
            props = get_field_properties(field.type)
            if props.is_unique:
                constraints.append(
                    UniqueConstraint(
                        LocalId(self.create_unique_key_name(cls, field.name)),
                        (LocalId(field.name),),
                    )
                )

        if self.options.foreign_constraints:
            for field in dataclass_fields_as_required(cls):
                if is_entity_type(field.type):
                    # foreign keys
                    constraints.append(
                        ForeignConstraint(
                            LocalId(self.create_foreign_key_name(cls, field.name)),
                            (LocalId(field.name),),
                            ConstraintReference(
                                self.create_qualified_id(
                                    field.type.__module__, field.type.__name__
                                ),
                                (LocalId(dataclass_primary_key_name(field.type)),),
                            ),
                        )
                    )

                if is_type_union(field.type):
                    member_types = [
                        evaluate_member_type(t, cls)
                        for t in unwrap_union_types(unwrap_annotated_type(field.type))
                    ]
                    if all(is_entity_type(t) for t in member_types):
                        # discriminated keys
                        constraints.append(
                            DiscriminatedConstraint(
                                LocalId(
                                    self.create_discriminated_key_name(cls, field.name)
                                ),
                                (LocalId(field.name),),
                                [
                                    ConstraintReference(
                                        self.create_qualified_id(
                                            t.__module__, t.__name__
                                        ),
                                        (LocalId(dataclass_primary_key_name(t)),),
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

        return self.options.factory.struct_class(
            name=self.create_qualified_id(cls.__module__, cls.__name__),
            members=members,
            description=doc.full_description,
        )

    def _enum_table(self, enum_type: type[enum.Enum]) -> Table:
        enum_table_name = enum_type.__name__
        id = self.create_qualified_id(enum_type.__module__, enum_table_name)
        columns = [
            self.options.factory.column_class(
                LocalId("id"),
                self._enumeration_key_type(),
                False,
                identity=True,
            )
        ]
        primary_key = (LocalId("id"),)
        constraints: list[Constraint] = []

        enum_type = unwrap_annotated_type(enum_type)
        enum_member_types: set[type] = set(type(e.value) for e in enum_type)
        if len(enum_member_types) > 1:
            raise TypeError(
                f"inconsistent member types in enumeration `{enum_type.__name__}`: {sorted(list(e.__name__ for e in enum_member_types))}"
            )
        enum_member_type = enum_member_types.pop()
        unadorned_member_type = unwrap_annotated_type(enum_member_type)

        if unadorned_member_type is int or unadorned_member_type is str:
            columns.append(
                self.options.factory.column_class(
                    LocalId("value"),
                    self.member_to_sql_data_type(ENUM_LABEL_TYPE, type(None)),
                    False,
                )
            )
            constraints.append(
                UniqueConstraint(
                    LocalId(f"uq_{enum_table_name}"),
                    (LocalId("value"),),
                )
            )
        elif is_dataclass_type(unadorned_member_type):
            columns.extend(
                self.member_to_column(
                    field, enum_member_type, parse_type(unadorned_member_type)
                )
                for field in dataclass_fields(unadorned_member_type)
            )
        else:
            raise TypeError(
                f"unsupported member type in enumeration `{enum_type.__name__}`: {enum_member_type}"
            )

        if self.options.initialize_tables:
            return self.options.factory.enum_table_class(
                id,
                columns,
                values=[e.value for e in enum_type],
                primary_key=primary_key,
                constraints=constraints,
            )
        else:
            return self.options.factory.table_class(
                id,
                columns,
                primary_key=primary_key,
                constraints=constraints,
            )

    def dataclasses_to_catalog(
        self, entity_types: list[type[DataclassInstance]]
    ) -> Catalog:
        "Converts a list of Python data-class types into a database object catalog."

        # omit abstract base classes
        entity_types = [t for t in entity_types if not inspect.isabstract(t)]

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
                enum_values = [str(e.value) for e in enum_type]  # type: ignore
                if len(enum_values) < 2:
                    # enumerations with too few members expand into extensible enumeration tables
                    continue

                enum_defs = enums.setdefault(enum_type.__module__, [])
                enum_defs.append(
                    EnumType(
                        QualifiedId(
                            self.options.namespaces.get(enum_type.__module__),
                            enum_type.__name__,
                        ),
                        enum_values,
                    )
                )

        # collect all struct types in alphabetical order
        structs: dict[str, list[StructType]] = {}
        if self.options.struct_mode is StructMode.TYPE:
            struct_types = [obj for obj in referenced_types if is_struct_type(obj)]
            struct_types.sort(key=lambda s: s.__name__)
            struct_types = _topological_sort(struct_types)
            for struct_type in struct_types:
                struct_defs = structs.setdefault(struct_type.__module__, [])
                struct_defs.append(self.dataclass_to_struct(struct_type))

        # create tables
        tables: dict[str, list[Table]] = {}
        table_types = [obj for obj in referenced_types if is_entity_type(obj)]
        table_types.sort(key=lambda t: t.__name__)
        for table_type in table_types:
            table_defs = tables.setdefault(table_type.__module__, [])
            table_defs.append(self.dataclass_to_table(table_type))

        # discover regular enumerations
        regular_enum_types: set[type[enum.Enum]] = set()
        if self.options.enum_mode is EnumMode.RELATION:
            for entity in table_types:
                # field of type E
                for enum_field in dataclass_fields_as_required(entity, is_type_enum):
                    regular_enum_types.add(enum_field.type)

                # field of type list[E]
                for enum_field in dataclass_fields_as_required(
                    entity, is_type_enum_list
                ):
                    regular_enum_types.add(unwrap_generic_list(enum_field.type))

                # field of type set[E]
                for enum_field in dataclass_fields_as_required(
                    entity, is_type_enum_set
                ):
                    regular_enum_types.add(unwrap_generic_set(enum_field.type))

        for enum_type in sorted(list(regular_enum_types), key=lambda e: e.__name__):
            table_defs = tables.setdefault(enum_type.__module__, [])
            table_defs.append(self._enum_table(enum_type))

        # discover extensible enumerations
        extensible_enum_types: set[type[enum.Enum]] = set()
        for entity in table_types:
            for enum_field in dataclass_fields_as_required(
                entity, lambda t: is_extensible_enum_type(t, entity)
            ):
                enum_type = unwrap_extensible_enum_type(enum_field.type, entity)
                extensible_enum_types.add(enum_type)
        for enum_type in sorted(list(extensible_enum_types), key=lambda e: e.__name__):
            table_defs = tables.setdefault(enum_type.__module__, [])
            table_defs.append(self._enum_table(enum_type))

        # create join tables for many-to-many relationships
        for entity in table_types:
            for field in dataclass_fields(entity):
                relationship = self._get_relationship(field.type)
                if relationship is None:
                    continue

                relationship_type, item_type = relationship

                # "primary" refers to the primary key name/type of the tables participating in the many-to-many relationship
                # "join" refers to the join table column names
                primary_left_name = LocalId(dataclass_primary_key_name(entity))
                if is_entity_type(item_type):
                    primary_right_name = LocalId(dataclass_primary_key_name(item_type))
                    primary_right_type = self.member_to_sql_data_type(
                        dataclass_primary_key_type(item_type), item_type
                    )
                elif self.options.enum_mode is EnumMode.RELATION and is_type_enum(
                    item_type
                ):
                    # use the same name and type as used when generating the enumeration table
                    primary_right_name = LocalId("id")
                    primary_right_type = self._enumeration_key_type()
                else:
                    # related type must be an entity, or (depending on options) an enumeration type
                    raise TypeError(f"unrecognized join relation type: {item_type}")

                column_left_name = f"{entity.__name__}_{field.name}"
                column_right_name = f"{item_type.__name__}_{primary_right_name.id}"
                table_name = f"{column_left_name}_{item_type.__name__}"
                join_left_name = f"{self.create_qualified_prefix(entity)}_{field.name}"
                join_right_name = f"{join_left_name}_{item_type.__name__}"

                constraints: list[Constraint] = [
                    ForeignConstraint(
                        LocalId(f"jk_{join_left_name}"),
                        (LocalId(column_left_name),),
                        ConstraintReference(
                            self.create_qualified_id(
                                entity.__module__,
                                entity.__name__,
                            ),
                            (primary_left_name,),
                        ),
                    ),
                    ForeignConstraint(
                        LocalId(f"jk_{join_right_name}"),
                        (LocalId(column_right_name),),
                        ConstraintReference(
                            self.create_qualified_id(
                                item_type.__module__,
                                item_type.__name__,
                            ),
                            (primary_right_name,),
                        ),
                    ),
                ]
                if relationship_type is set:
                    constraints.append(
                        UniqueConstraint(
                            LocalId(f"uq_{join_right_name}"),
                            (LocalId(column_right_name),),
                        )
                    )

                table_defs = tables.setdefault(entity.__module__, [])
                table_defs.append(
                    self.options.factory.table_class(
                        self.create_qualified_id(
                            entity.__module__,
                            table_name,
                        ),
                        [
                            self.options.factory.column_class(
                                LocalId("uuid"),
                                self.member_to_sql_data_type(uuid.UUID, entity),
                                False,
                            ),
                            self.options.factory.column_class(
                                LocalId(column_left_name),
                                self.member_to_sql_data_type(
                                    dataclass_primary_key_type(entity), entity
                                ),
                                False,
                            ),
                            self.options.factory.column_class(
                                LocalId(column_right_name),
                                primary_right_type,
                                False,
                            ),
                        ],
                        primary_key=(LocalId("uuid"),),
                        constraints=constraints,
                    )
                )

        if self.options.qualified_names:
            namespaces = [
                self.options.factory.namespace_class(
                    name=LocalId(self.options.namespaces.get(module_name) or ""),
                    enums=enums.get(module_name, []),
                    structs=structs.get(module_name, []),
                    tables=table_defs,
                )
                for module_name, table_defs in tables.items()
            ]
        else:
            namespaces = [
                self.options.factory.namespace_class(
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
