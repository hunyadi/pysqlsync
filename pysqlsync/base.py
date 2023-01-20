import abc
import dataclasses
import enum
import typing
from dataclasses import dataclass
from io import StringIO
from typing import Annotated, Any, Callable, Iterable, Optional, TextIO, TypeVar


from strong_typing.inspection import get_annotation, is_type_enum

T = TypeVar("T")


def get_extractor(field_name: str, field_type: type) -> Callable[[Any], Any]:
    if is_type_enum(field_type):
        return lambda obj: getattr(obj, field_name).value
    else:
        return lambda obj: getattr(obj, field_name)


def get_extractors(class_type: type) -> tuple[Callable[[Any], Any], ...]:
    return tuple(
        get_extractor(field.name, field.type)
        for field in dataclasses.fields(class_type)
    )


class BaseGenerator(abc.ABC):
    cls: type

    def __init__(self, cls: type) -> None:
        self.cls = cls

    def write_create_table_stmt(self, target: TextIO) -> None:
        ...

    def write_insert_stmt(self, target: TextIO) -> None:
        ...

    def get_create_table_stmt(self) -> str:
        s = StringIO()
        self.write_create_table_stmt(s)
        return s.getvalue()

    def get_insert_stmt(self) -> str:
        s = StringIO()
        self.write_insert_stmt(s)
        return s.getvalue()

    def get_record_as_tuple(self, obj: Any) -> tuple:
        extractors = get_extractors(self.cls)
        return tuple(extractor(obj) for extractor in extractors)

    def get_records_as_tuples(self, items: Iterable[Any]) -> list[tuple]:
        extractors = get_extractors(self.cls)
        return [tuple(extractor(item) for extractor in extractors) for item in items]


@dataclass
class Parameters:
    host: Optional[str]
    port: Optional[int]
    username: Optional[str]
    password: Optional[str]
    database: Optional[str]


class BaseConnection(abc.ABC):
    params: Parameters

    def __init__(self, params: Parameters) -> None:
        self.params = params

    @abc.abstractmethod
    async def __aenter__(self) -> "BaseContext":
        ...


class BaseContext(abc.ABC):
    @abc.abstractmethod
    async def execute(self, statement: str) -> None:
        ...

    @abc.abstractmethod
    async def execute_all(
        self, statement: str, args: Iterable[tuple[Any, ...]]
    ) -> None:
        ...


class BaseEngine(abc.ABC):
    @abc.abstractmethod
    def get_generator_type(self) -> type[BaseGenerator]:
        ...

    @abc.abstractmethod
    def get_connection_type(self) -> type[BaseConnection]:
        ...


class PrimaryKeyTag:
    "Marks a field as the primary key of a table."


PrimaryKey = Annotated[T, PrimaryKeyTag()]


def is_primary_key_type(field_type: type) -> bool:
    "Checks if the field type is marked as the primary key of a table."

    return get_annotation(field_type, PrimaryKeyTag) is not None


def get_primary_key_name(class_type: type) -> str:
    "Fetches the primary key of the table."

    for field in dataclasses.fields(class_type):
        if is_primary_key_type(field.type):
            return field.name

    raise TypeError(f"table type has no primary key: {class_type.__name__}")


def is_constraint(item: Any) -> bool:
    return isinstance(item, PrimaryKeyTag)


@dataclass
class FieldProperties:
    """
    Captures type information associated with a field type.

    :param field_type: Type without constraint annotations such as identity, primary key, or unique.
    :param inner_type: Type with no metadata (annotations).
    :param metadata: Any metadata that is not a constraint such as identity, primary key or unique.
    :param is_primary: True if the field is a primary key.
    """

    field_type: type
    inner_type: type
    metadata: list
    is_primary: bool


def get_field_properties(field_type: type) -> FieldProperties:
    metadata = getattr(field_type, "__metadata__", None)
    if metadata is None:
        # field has a type without annotations or constraints
        return FieldProperties(field_type, field_type, [], False)

    # field has a type of Annotated[T, ...]
    inner_type = typing.get_args(field_type)[0]

    # check for constraints
    is_primary = is_primary_key_type(field_type)

    # filter annotations that represent constraints
    metadata = [item for item in metadata if not is_constraint(item)]

    if metadata:
        # type becomes Annotated[T, ...]
        outer_type: type = Annotated[(inner_type, *metadata)]  # type: ignore
    else:
        # type becomes a regular type
        outer_type = inner_type

    return FieldProperties(outer_type, inner_type, metadata, is_primary)
