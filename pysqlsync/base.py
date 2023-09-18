import abc
import dataclasses
import ipaddress
import types
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional, TypeVar

from strong_typing.inspection import DataclassInstance, is_dataclass_type, is_type_enum

D = TypeVar("D", bound=DataclassInstance)
T = TypeVar("T")


def _get_extractor(field_name: str, field_type: type) -> Callable[[Any], Any]:
    if is_type_enum(field_type):
        return lambda obj: getattr(obj, field_name).value
    elif field_type is ipaddress.IPv4Address or field_type is ipaddress.IPv6Address:
        return lambda obj: str(getattr(obj, field_name))
    else:
        return lambda obj: getattr(obj, field_name)


def _get_extractors(class_type: type) -> tuple[Callable[[Any], Any], ...]:
    return tuple(
        _get_extractor(field.name, field.type)
        for field in dataclasses.fields(class_type)
    )


@dataclass
class GeneratorOptions:
    namespaces: dict[types.ModuleType, Optional[str]] = dataclasses.field(
        default_factory=dict
    )


class BaseGenerator(abc.ABC):
    "Generates SQL statements for creating or dropping tables, and inserting, updating or deleting data."

    cls: type
    options: GeneratorOptions

    def __init__(self, cls: type, options: GeneratorOptions) -> None:
        self.cls = cls
        self.options = options

    @abc.abstractmethod
    def get_create_stmt(self) -> str:
        ...

    @abc.abstractmethod
    def get_drop_stmt(self) -> str:
        ...

    @abc.abstractmethod
    def get_upsert_stmt(self) -> str:
        ...

    def get_record_as_tuple(self, obj: Any) -> tuple:
        extractors = _get_extractors(self.cls)
        return tuple(extractor(obj) for extractor in extractors)

    def get_records_as_tuples(self, items: Iterable[Any]) -> list[tuple]:
        extractors = _get_extractors(self.cls)
        return [tuple(extractor(item) for extractor in extractors) for item in items]


@dataclass
class ConnectionParameters:
    "Database connection parameters that would typically be encapsulated in a connection string."

    host: Optional[str]
    port: Optional[int]
    username: Optional[str]
    password: Optional[str]
    database: Optional[str]


class BaseConnection(abc.ABC):
    "An active connection to a database."

    create_generator: Callable[[type], BaseGenerator]
    params: ConnectionParameters

    def __init__(
        self,
        create_generator: Callable[[type], BaseGenerator],
        params: ConnectionParameters,
    ) -> None:
        self.create_generator = create_generator
        self.params = params

    @abc.abstractmethod
    async def __aenter__(self) -> "BaseContext":
        ...

    @abc.abstractmethod
    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> None:
        ...


class BaseContext(abc.ABC):
    "Context object returned by a connection object."

    connection: BaseConnection

    def __init__(self, connection: BaseConnection) -> None:
        self.connection = connection

    @abc.abstractmethod
    async def execute(self, statement: str) -> None:
        ...

    @abc.abstractmethod
    async def query_all(self, signature: type[T], statement: str) -> list[T]:
        ...

    @abc.abstractmethod
    async def execute_all(
        self, statement: str, args: Iterable[tuple[Any, ...]]
    ) -> None:
        ...

    async def drop_table(self, table: type[DataclassInstance]) -> None:
        "Drops a database table corresponding to the dataclass type."

        if not is_dataclass_type(table):
            raise TypeError(f"expected dataclass type, got: {table}")
        generator = self.connection.create_generator(table)
        statement = generator.get_drop_stmt()
        await self.execute(statement)

    async def create_table(self, table: type[DataclassInstance]) -> None:
        "Creates a database table for storing data encapsulated in the dataclass type."

        if not is_dataclass_type(table):
            raise TypeError(f"expected dataclass type, got: {table}")
        generator = self.connection.create_generator(table)
        statement = generator.get_create_stmt()
        await self.execute(statement)

    async def insert_data(self, table: type[D], data: Iterable[D]) -> None:
        "Inserts data in the database table corresponding to the dataclass type."

        return await self.upsert_data(table, data)

    async def upsert_data(self, table: type[D], data: Iterable[D]) -> None:
        "Inserts or updates data in the database table corresponding to the dataclass type."

        generator = self.connection.create_generator(table)
        statement = generator.get_upsert_stmt()
        records = generator.get_records_as_tuples(data)
        await self.execute_all(statement, records)


class BaseEngine(abc.ABC):
    "Represents a specific database server type."

    @abc.abstractmethod
    def get_generator_type(self) -> type[BaseGenerator]:
        ...

    @abc.abstractmethod
    def get_connection_type(self) -> type[BaseConnection]:
        ...

    def create_connection(
        self, params: ConnectionParameters, options: GeneratorOptions
    ) -> BaseConnection:
        connection_type = self.get_connection_type()
        return connection_type(lambda cls: self.create_generator(cls, options), params)

    def create_generator(self, cls: type, options: GeneratorOptions) -> BaseGenerator:
        generator_type = self.get_generator_type()
        return generator_type(cls, options)
