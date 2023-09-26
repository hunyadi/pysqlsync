import abc
import dataclasses
import ipaddress
import types
import typing
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional, TypeVar

from strong_typing.inspection import DataclassInstance, is_dataclass_type, is_type_enum

from .formation.object_types import Table
from .model.id_types import LocalId, QualifiedId

D = TypeVar("D", bound=DataclassInstance)
T = TypeVar("T")


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
        extractors = self.get_extractors(self.cls)
        return tuple(extractor(obj) for extractor in extractors)

    def get_records_as_tuples(self, items: Iterable[Any]) -> list[tuple]:
        extractors = self.get_extractors(self.cls)
        return [tuple(extractor(item) for extractor in extractors) for item in items]

    def get_extractor(self, field_name: str, field_type: type) -> Callable[[Any], Any]:
        if is_type_enum(field_type):
            return lambda obj: getattr(obj, field_name).value
        elif field_type is ipaddress.IPv4Address or field_type is ipaddress.IPv6Address:
            return lambda obj: str(getattr(obj, field_name))
        else:
            return lambda obj: getattr(obj, field_name)

    def get_extractors(self, class_type: type) -> tuple[Callable[[Any], Any], ...]:
        return tuple(
            self.get_extractor(field.name, field.type)
            for field in dataclasses.fields(class_type)
        )


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
        "Run a query to produce a result-set of multiple columns."

        ...

    def _resultset_unwrap_dict(
        self, signature: D, records: Iterable[dict[str, Any]]
    ) -> list[D]:
        if is_dataclass_type(signature):
            return [signature(**{name: value for name, value in record.items()}) for record in records]  # type: ignore

        raise TypeError(f"illegal resultset signature: {signature}")

    def _resultset_unwrap_tuple(
        self, signature: type[T], records: Iterable[tuple[Any, ...]]
    ) -> list[T]:
        if signature in [bool, int, float, str]:
            return [row[0] for row in records]

        if is_dataclass_type(signature):
            raise TypeError(
                f"data-class type expects dictionary of records: {signature}"
            )

        origin_type = typing.get_origin(signature)
        if origin_type is tuple:
            return [tuple(row) for row in records]  # type: ignore

        raise TypeError(f"illegal resultset signature: {signature}")

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


class Explorer(abc.ABC):
    conn: BaseContext

    def __init__(self, conn: BaseContext) -> None:
        self.conn = conn

    @abc.abstractmethod
    async def get_table_names(self) -> list[QualifiedId]:
        ...

    @abc.abstractmethod
    async def has_table(self, table_id: QualifiedId) -> bool:
        ...

    @abc.abstractmethod
    async def has_column(self, table_id: QualifiedId, column_id: LocalId) -> bool:
        ...

    @abc.abstractmethod
    async def get_table_meta(self, table_id: QualifiedId) -> Table:
        ...


class BaseEngine(abc.ABC):
    "Represents a specific database server type."

    @abc.abstractmethod
    def get_generator_type(self) -> type[BaseGenerator]:
        ...

    @abc.abstractmethod
    def get_connection_type(self) -> type[BaseConnection]:
        ...

    @abc.abstractmethod
    def get_explorer_type(self) -> type[Explorer]:
        ...

    def create_connection(
        self, params: ConnectionParameters, options: Optional[GeneratorOptions] = None
    ) -> BaseConnection:
        "Opens a connection to a database server."

        generator_options = options if options is not None else GeneratorOptions()
        connection_type = self.get_connection_type()
        return connection_type(
            lambda cls: self.create_generator(cls, generator_options), params
        )

    def create_generator(self, cls: type, options: GeneratorOptions) -> BaseGenerator:
        "Instantiates a generator that can emit SQL statements."

        generator_type = self.get_generator_type()
        return generator_type(cls, options)

    def create_explorer(self, conn: BaseContext) -> Explorer:
        explorer_type = self.get_explorer_type()
        return explorer_type(conn)
