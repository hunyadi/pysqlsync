import abc
from dataclasses import dataclass
from io import StringIO
from typing import Any, Generic, Iterable, Optional, TextIO, TypeVar


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


T = TypeVar("T")


@dataclass
class PrimaryKey(Generic[T]):
    pass
