import abc
import dataclasses
import enum
import typing
from dataclasses import dataclass
from io import StringIO
from typing import Any, Generic, Iterable, Optional, TextIO, TypeVar


T = TypeVar("T")


def get_attribute_value(obj: Any, name: str) -> Any:
    value = getattr(obj, name)
    if isinstance(value, enum.Enum):
        return value.value
    else:
        return value


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
        if not isinstance(obj, self.cls):
            raise TypeError(f"mismatching type; expected: {self.cls}, got: {type(obj)}")

        return tuple(
            get_attribute_value(obj, field.name)
            for field in dataclasses.fields(self.cls)
        )

    def get_records_as_tuples(self, items: list[Any]) -> list[tuple]:
        if not isinstance(items, list):
            return TypeError(f"expected list of objects but got: {type(items)}")
        if not items:
            return []
        return [self.get_record_as_tuple(item) for item in items]


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


@dataclass
class PrimaryKey(Generic[T]):
    pass


def get_primary_key(cls: type) -> tuple[str, type]:
    for field in dataclasses.fields(cls):
        if typing.get_origin(field.type) is PrimaryKey:
            return field.name, typing.get_args(field.type)[0]

    raise TypeError(f"type has no primary key: {cls.__name__}")
