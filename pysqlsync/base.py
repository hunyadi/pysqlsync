import abc
import dataclasses
import ipaddress
import types
import typing
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional, TypeVar

from strong_typing.inspection import DataclassInstance, is_dataclass_type, is_type_enum

from .formation.object_types import Catalog, Column, Table
from .formation.py_to_sql import DataclassConverter, EnumMode
from .model.id_types import LocalId, QualifiedId, SupportsQualifiedId

D = TypeVar("D", bound=DataclassInstance)
T = TypeVar("T")


@dataclass
class GeneratorOptions:
    """
    Database-agnostic generator options.

    :param enum_mode: Conversion mode for enumeration types.
    :param namespaces: Maps Python modules into SQL namespaces (a.k.a. schemas).
    """

    enum_mode: Optional[EnumMode] = None
    namespaces: dict[types.ModuleType, Optional[str]] = dataclasses.field(
        default_factory=dict
    )


class BaseGenerator(abc.ABC):
    """
    Generates SQL statements for creating or dropping tables, and inserting, updating or deleting data.

    :param options: Database-agnostic generator options.
    :param converter: A converter that maps a set of data-class types into a database state.
    :param state: A current database state based on which statements are generated.
    """

    options: GeneratorOptions
    converter: DataclassConverter
    state: Catalog

    def __init__(self, options: GeneratorOptions) -> None:
        self.options = options
        self.catalog = Catalog([])

    @property
    def column_class(self) -> type[Column]:
        return Column

    def create(self, tables: list[type[DataclassInstance]]) -> Optional[str]:
        target = self.converter.dataclasses_to_catalog(tables)
        statement = self.get_mutate_stmt(target)
        self.catalog = target
        return statement

    def drop(self) -> Optional[str]:
        target = Catalog([])
        statement = self.get_mutate_stmt(target)
        self.catalog = target
        return statement

    def get_mutate_stmt(self, target: Catalog) -> Optional[str]:
        "Returns a SQL statement to mutate a source state into a target state."

        return target.mutate_stmt(self.catalog)

    @abc.abstractmethod
    def get_table_insert_stmt(self, table: Table) -> str:
        "Returns a SQL statement to insert or ignore records in a database table."

        ...

    @abc.abstractmethod
    def get_table_upsert_stmt(self, table: Table) -> str:
        "Returns a SQL statement to insert or update records in a database table."

        ...

    def get_qualified_id(self, table: type[DataclassInstance]) -> SupportsQualifiedId:
        return self.converter.create_qualified_id(table.__module__, table.__name__)

    def get_dataclass_upsert_stmt(self, table: type[DataclassInstance]) -> str:
        "Returns a SQL statement to insert records into a database table."

        table_object = self.catalog.get_table(self.get_qualified_id(table))
        return self.get_table_upsert_stmt(table_object)

    def get_dataclass_as_record(self, item: DataclassInstance) -> tuple:
        "Converts a data-class object into a record to insert into a database table."

        extractors = self.get_dataclass_extractors(item.__class__)
        return tuple(extractor(item) for extractor in extractors)

    def get_dataclasses_as_records(
        self, items: Iterable[DataclassInstance]
    ) -> list[tuple]:
        "Converts a list of data-class objects into a list of records to insert into a database table."

        it = iter(items)
        item = next(it)
        extractors = self.get_dataclass_extractors(item.__class__)

        results = [tuple(extractor(item) for extractor in extractors)]
        while True:
            try:
                item = next(it)
            except StopIteration:
                return results
            results.append(tuple(extractor(item) for extractor in extractors))

    def get_dataclass_extractors(
        self, class_type: type
    ) -> tuple[Callable[[Any], Any], ...]:
        "Returns a tuple of callable function objects that extracts each field of a data-class."

        return tuple(
            self.get_field_extractor(field.name, field.type)
            for field in dataclasses.fields(class_type)
        )

    def get_field_extractor(
        self, field_name: str, field_type: type
    ) -> Callable[[Any], Any]:
        "Returns a callable function object that extracts a single field of a data-class."

        if is_type_enum(field_type):
            return lambda obj: getattr(obj, field_name).value
        elif field_type is ipaddress.IPv4Address or field_type is ipaddress.IPv6Address:
            return lambda obj: str(getattr(obj, field_name))
        else:
            return lambda obj: getattr(obj, field_name)

    def get_value_extractor(self, field_type: type) -> Optional[Callable[[Any], Any]]:
        "Returns a callable function object that extracts a single field of a data-class."

        if is_type_enum(field_type):
            return lambda field: field.value
        elif field_type is ipaddress.IPv4Address or field_type is ipaddress.IPv6Address:
            return lambda field: str(field)
        else:
            return None

    def get_enum_extractor(self, enum_dict: dict[Any, int]) -> Callable[[Any], Any]:
        return lambda field: enum_dict[field]


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

    generator: BaseGenerator
    params: ConnectionParameters

    def __init__(
        self,
        generator: BaseGenerator,
        params: ConnectionParameters,
    ) -> None:
        self.generator = generator
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
        "Executes one or more SQL statements."

        ...

    async def query_one(self, signature: type[T], statement: str) -> T:
        "Runs a query to produce a result-set of one or more columns, and a single row."

        rows = await self.query_all(signature, statement)
        return rows[0]

    @abc.abstractmethod
    async def query_all(self, signature: type[T], statement: str) -> list[T]:
        "Runs a query to produce a result-set of one or more columns, and multiple rows."

        ...

    def _resultset_unwrap_dict(
        self, signature: type[D], records: Iterable[dict[str, Any]]
    ) -> list[D]:
        """Converts a result-set into a list of data-class instances.

        :param signature: A data-class type.
        :param records: The result-set whose rows to convert.
        """

        if is_dataclass_type(signature):
            return [signature(**{name: value for name, value in record.items()}) for record in records]  # type: ignore

        raise TypeError(
            f"expected: data-class type as result-set signature; got: {signature}"
        )

    def _resultset_unwrap_tuple(
        self, signature: type[T], records: Iterable[Sequence[Any]]
    ) -> list[T]:
        """
        Converts a result-set into a list of tuples, or a list of simple types (as appropriate).

        :param signature: A tuple type, or a simple type (e.g. `bool` or `str`).
        :param records: The result-set whose rows to convert.
        """

        if signature in [bool, int, float, str]:
            scalar_results: list[T] = []

            # check result shape
            it = iter(records)
            item = next(it)
            if len(item) != 1:
                raise ValueError(
                    f"invalid number of columns, expected: 1; got: {len(item)}"
                )
            scalar_results.append(item[0])
            while True:
                try:
                    item = next(it)
                except StopIteration:
                    return scalar_results
                scalar_results.append(item[0])

        origin_type = typing.get_origin(signature)
        if origin_type is tuple:
            origin_args = typing.get_args(signature)
            results: list[T] = []

            # check result shape
            it = iter(records)
            item = next(it)
            if len(item) != len(origin_args):
                raise ValueError(
                    f"invalid number of columns, expected: {len(origin_args)}; got: {len(item)}"
                )

            if isinstance(item, tuple):
                results.append(item)  # type: ignore
                while True:
                    try:
                        item = next(it)
                    except StopIteration:
                        return results
                    results.append(item)  # type: ignore
            else:
                results.append(tuple(item))  # type: ignore
                while True:
                    try:
                        item = next(it)
                    except StopIteration:
                        return results
                    results.append(tuple(item))  # type: ignore

        raise TypeError(
            f"expected: tuple or simple type as result-set signature; got: {signature}"
        )

    @abc.abstractmethod
    async def execute_all(
        self, statement: str, args: Iterable[tuple[Any, ...]]
    ) -> None:
        ...

    def get_table(self, table: type[DataclassInstance]) -> Table:
        return self.connection.generator.catalog.get_table(
            self.connection.generator.get_qualified_id(table)
        )

    async def create_objects(self, tables: list[type[DataclassInstance]]) -> None:
        generator = self.connection.generator
        statement = generator.create(tables)
        if statement:
            await self.execute(statement)

    async def drop_objects(self) -> None:
        generator = self.connection.generator
        statement = generator.drop()
        if statement:
            await self.execute(statement)

    async def insert_data(self, table: type[D], data: Iterable[D]) -> None:
        "Inserts data in the database table corresponding to the dataclass type."

        return await self.upsert_data(table, data)

    async def upsert_data(self, table: type[D], data: Iterable[D]) -> None:
        "Inserts or updates data in the database table corresponding to the dataclass type."

        generator = self.connection.generator
        statement = generator.get_dataclass_upsert_stmt(table)
        records = generator.get_dataclasses_as_records(data)
        await self.execute_all(statement, records)

    async def upsert_rows(
        self,
        table: Table,
        signature: tuple[type, ...],
        records: Iterable[tuple[Any, ...]],
    ) -> None:
        """
        Inserts or updates rows in a database table.

        :param table: The table to be updated.
        :param signature: The data types of the items in a row record.
        :param records: The rows to be inserted into or updated in the database table.
        """

        generator = self.connection.generator

        extractors: list[Optional[Callable[[Any], Any]]] = []
        for index, column, value_type in zip(
            range(len(table.columns)), table.columns.values(), signature
        ):
            extractor = generator.get_value_extractor(value_type)
            if value_type is str and table.is_relation(column.name):
                relation = generator.catalog.get_referenced_table(
                    table.name, column.name
                )
                if relation.is_lookup_table():
                    enum_dict: dict[Any, int] = await self._merge_lookup_table(
                        relation, set(record[index] for record in records)
                    )
                    extractor = generator.get_enum_extractor(enum_dict)
            extractors.append(extractor)

        if all(extractor is None for extractor in extractors):
            record_generator = records
        else:
            record_generator = (
                tuple(
                    (extractor(field) if extractor is not None else field)
                    for extractor, field in zip(extractors, record)
                )
                for record in records
            )

        statement = generator.get_table_upsert_stmt(table)
        await self.execute_all(
            statement,
            record_generator,
        )

    async def _merge_lookup_table(
        self, table: Table, values: Iterable[str]
    ) -> dict[Any, int]:
        "Merges new values into a lookup table and returns the entire updated table."

        await self.execute_all(
            self.connection.generator.get_table_insert_stmt(table),
            list((value,) for value in values),
        )
        column_names = ", ".join(  # join on one element
            str(column.name) for column in table.get_value_columns()
        )
        results = await self.query_all(
            tuple[str, int],
            f"SELECT {column_names}, {table.get_primary_column().name} FROM {table.name}",
        )
        return dict(results)  # type: ignore


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

    @abc.abstractproperty
    def name(self) -> str:
        ...

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
        return connection_type(self.create_generator(generator_options), params)

    def create_generator(self, options: GeneratorOptions) -> BaseGenerator:
        "Instantiates a generator that can emit SQL statements."

        generator_type = self.get_generator_type()
        return generator_type(options)

    def create_explorer(self, conn: BaseContext) -> Explorer:
        explorer_type = self.get_explorer_type()
        return explorer_type(conn)
