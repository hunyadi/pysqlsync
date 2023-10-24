import abc
import dataclasses
import json
import logging
import types
import typing
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional, Sized, TypeVar, overload

from strong_typing.inspection import DataclassInstance, is_dataclass_type, is_type_enum
from strong_typing.name import python_type_to_str

from .formation.inspection import get_entity_types
from .formation.object_types import Catalog, Column, Namespace, ObjectFactory, Table
from .formation.py_to_sql import DataclassConverter, EnumMode, StructMode
from .model.data_types import SqlJsonType
from .model.id_types import LocalId, QualifiedId, SupportsQualifiedId

D = TypeVar("D", bound=DataclassInstance)
T = TypeVar("T")

LOGGER = logging.getLogger("pysqlsync")

_JSON_ENCODER = json.JSONEncoder(
    ensure_ascii=False,
    check_circular=False,
    allow_nan=False,
    indent=None,
    separators=(",", ":"),
)


@dataclass
class GeneratorOptions:
    """
    Database-agnostic generator options.

    :param enum_mode: Conversion mode for enumeration types.
    :param struct_mode: Conversion mode for composite types.
    :param namespaces: Maps Python modules into SQL namespaces (a.k.a. schemas).
    :param foreign_constraints: Whether to create foreign/primary key relationships between tables.
    :param skip_annotations: Annotation classes to ignore on table column types.
    """

    enum_mode: Optional[EnumMode] = None
    struct_mode: Optional[StructMode] = None
    namespaces: dict[types.ModuleType, Optional[str]] = dataclasses.field(
        default_factory=dict
    )
    foreign_constraints: bool = True
    skip_annotations: tuple[type, ...] = ()


class BaseGenerator(abc.ABC):
    """
    Generates SQL statements for creating or dropping tables, and inserting, updating or deleting data.

    :param options: Database-agnostic generator options.
    :param factory: A factory object to create new column, table, struct and namespace instances.
    :param converter: A converter that maps a set of data-class types into a database state.
    :param state: A current database state based on which statements are generated.
    """

    options: GeneratorOptions
    factory: ObjectFactory
    converter: DataclassConverter
    state: Catalog

    def __init__(
        self, options: GeneratorOptions, factory: Optional[ObjectFactory] = None
    ) -> None:
        self.options = options
        self.factory = factory if factory is not None else ObjectFactory()
        self.reset()

    def reset(self) -> None:
        self.state = Catalog([])

    @overload
    def create(self, *, tables: list[type[DataclassInstance]]) -> Optional[str]:
        ...

    @overload
    def create(self, *, modules: list[types.ModuleType]) -> Optional[str]:
        ...

    def create(
        self,
        *,
        tables: Optional[list[type[DataclassInstance]]] = None,
        modules: Optional[list[types.ModuleType]] = None,
    ) -> Optional[str]:
        """
        Creates tables, structs, enumerations, constraints, etc. corresponding to entity class definitions.

        :param tables: The list of entity classes to define the SQL objects.
        :param modules: The list of modules whose entity classes defined the SQL objects.
        :returns: A SQL statement that creates tables, structs, enumerations, constraints, etc.
        """

        if tables is not None and modules is not None:
            raise TypeError("arguments `tables` and `modules` are exclusive")
        if tables is None and modules is None:
            raise TypeError("one of arguments `tables` and `modules` is required")

        if tables:
            target = self.converter.dataclasses_to_catalog(tables)
            statement = self.get_mutate_stmt(target)
            self.state = target
            return statement
        elif modules:
            target = self.converter.modules_to_catalog(modules)
            statement = self.get_mutate_stmt(target)
            self.state = target
            return statement
        else:
            raise NotImplementedError()

    def drop(self) -> Optional[str]:
        """
        Drops all objects currently synced with the generator.

        (This function is mainly for unit tests.)

        :returns: A SQL statement that drops tables, structs, enumerations, constraints, etc.
        """

        target = Catalog([])
        statement = self.get_mutate_stmt(target)
        self.state = target
        return statement

    def get_mutate_stmt(self, target: Catalog) -> Optional[str]:
        "Returns a SQL statement to mutate a source state into a target state."

        return target.mutate_stmt(self.state)

    def get_table_insert_stmt(self, table: Table) -> str:
        "Returns a SQL statement to insert new records in a database table."

        return self.get_table_merge_stmt(table)

    @abc.abstractmethod
    def get_table_merge_stmt(self, table: Table) -> str:
        "Returns a SQL statement to insert or ignore records in a database table."

        ...

    @abc.abstractmethod
    def get_table_upsert_stmt(self, table: Table) -> str:
        "Returns a SQL statement to insert or update records in a database table."

        ...

    def get_qualified_id(self, table: type[DataclassInstance]) -> SupportsQualifiedId:
        return self.converter.create_qualified_id(table.__module__, table.__name__)

    def get_dataclass_insert_stmt(self, table: type[DataclassInstance]) -> str:
        "Returns a SQL statement to insert or ignore records in a database table."

        table_object = self.state.get_table(self.get_qualified_id(table))
        return self.get_table_insert_stmt(table_object)

    def get_dataclass_upsert_stmt(self, table: type[DataclassInstance]) -> str:
        "Returns a SQL statement to insert or update records in a database table."

        table_object = self.state.get_table(self.get_qualified_id(table))
        return self.get_table_upsert_stmt(table_object)

    def get_dataclass_as_record(self, item: DataclassInstance) -> tuple:
        "Converts a data-class object into a record to insert into a database table."

        table_object = self.state.get_table(self.get_qualified_id(item.__class__))
        extractors = self._get_dataclass_extractors(table_object, item.__class__)
        return tuple(extractor(item) for extractor in extractors)

    def get_dataclasses_as_records(
        self, items: Iterable[DataclassInstance]
    ) -> list[tuple]:
        "Converts a list of data-class objects into a list of records to insert into a database table."

        it = iter(items)
        item = next(it)
        table_object = self.state.get_table(self.get_qualified_id(item.__class__))
        extractors = self._get_dataclass_extractors(table_object, item.__class__)

        results = [tuple(extractor(item) for extractor in extractors)]
        while True:
            try:
                item = next(it)
            except StopIteration:
                return results
            results.append(tuple(extractor(item) for extractor in extractors))

    def _get_dataclass_extractors(
        self, table: Table, class_type: type[DataclassInstance]
    ) -> tuple[Callable[[Any], Any], ...]:
        "Returns a tuple of callable function objects that extracts each field of a data-class."

        return tuple(
            self.get_field_extractor(table.columns[field.name], field.name, field.type)
            for field in dataclasses.fields(class_type)
        )

    def get_field_extractor(
        self, column: Column, field_name: str, field_type: type
    ) -> Callable[[Any], Any]:
        "Returns a callable function object that extracts a single field of a data-class."

        if is_type_enum(field_type):
            return lambda obj: getattr(obj, field_name).value
        else:
            return lambda obj: getattr(obj, field_name)

    def get_value_transformer(
        self, column: Column, field_type: type
    ) -> Optional[Callable[[Any], Any]]:
        "Returns a callable function object that transforms a value type into the expected column type."

        if isinstance(column.data_type, SqlJsonType):
            return lambda field: _JSON_ENCODER.encode(field)

        if is_type_enum(field_type):
            return lambda field: field.value
        else:
            return None

    def get_enum_transformer(self, enum_dict: dict[str, int]) -> Callable[[Any], Any]:
        "Returns a callable function object that looks up an assigned integer value based on the enumeration string value."

        return lambda field: enum_dict[field]


class QueryException(RuntimeError):
    query: str

    def __init__(self, query: str) -> None:
        super().__init__()
        self.query = query

    def __str__(self) -> str:
        return f"error executing query:\n{self.query}"


@dataclass
class ConnectionParameters:
    "Database connection parameters that would typically be encapsulated in a connection string."

    host: Optional[str]
    port: Optional[int]
    username: Optional[str]
    password: Optional[str]
    database: Optional[str]

    def __str__(self) -> str:
        host = self.host or "localhost"
        port = f":{self.port}" if self.port else ""
        username = f"{self.username}@" if self.username else ""
        database = f"/{self.database}" if self.database else ""
        return f"{username}{host}{port}{database}"


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

    async def execute(self, statement: str) -> None:
        "Executes one or more SQL statements."

        if not statement:
            raise ValueError("empty statement")

        LOGGER.debug(f"execute SQL:\n{statement}")
        try:
            await self._execute(statement)
        except Exception as e:
            raise QueryException(statement) from e

    @abc.abstractmethod
    async def _execute(self, statement: str) -> None:
        "Executes one or more SQL statements."

        ...

    async def execute_all(
        self, statement: str, args: Iterable[tuple[Any, ...]]
    ) -> None:
        "Executes a SQL statement with several records of data."

        if not statement:
            raise ValueError("empty statement")

        if isinstance(args, Sized):
            LOGGER.debug(f"execute SQL with {len(args)} rows:\n{statement}")
        else:
            LOGGER.debug(f"execute SQL:\n{statement}")
        try:
            await self._execute_all(statement, args)
        except Exception as e:
            raise QueryException(statement) from e

    @abc.abstractmethod
    async def _execute_all(
        self, statement: str, args: Iterable[tuple[Any, ...]]
    ) -> None:
        "Executes a SQL statement with several records of data."

        ...

    async def query_one(self, signature: type[T], statement: str) -> T:
        "Runs a query to produce a result-set of one or more columns, and a single row."

        rows = await self.query_all(signature, statement)
        return rows[0]

    async def query_all(self, signature: type[T], statement: str) -> list[T]:
        "Runs a query to produce a result-set of one or more columns, and multiple rows."

        LOGGER.debug(
            f"query SQL with into {python_type_to_str(signature)}:\n{statement}"
        )
        return await self._query_all(signature, statement)

    @abc.abstractmethod
    async def _query_all(self, signature: type[T], statement: str) -> list[T]:
        "Runs a query to produce a result-set of one or more columns, and multiple rows."

        ...

    def _resultset_unwrap_dict(
        self, signature: type[D], records: Iterable[dict[str, Any]]
    ) -> list[D]:
        """
        Converts a result-set into a list of data-class instances.

        :param signature: A data-class type.
        :param records: The result-set whose rows to convert.
        """

        if not is_dataclass_type(signature):
            raise TypeError(
                f"expected: data-class type as result-set signature; got: {signature}"
            )

        return [
            signature(**{name: value for name, value in record.items()})  # type: ignore
            for record in records
        ]

    def _resultset_unwrap_object(
        self, signature: type[D], records: Iterable[Any]
    ) -> list[D]:
        """
        Converts a result-set into a list of data-class instances.

        :param signature: A data-class type.
        :param records: The result-set whose rows to convert.
        """

        if not is_dataclass_type(signature):
            raise TypeError(
                f"expected: data-class type as result-set signature; got: {signature}"
            )

        names = [name for name in signature.__dataclass_fields__.keys()]
        return [
            signature(**{name: record.__getattribute__(name) for name in names}) for record in records  # type: ignore
        ]

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
            try:
                item = next(it)
            except StopIteration:
                return []
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
            try:
                item = next(it)
            except StopIteration:
                return []
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

    async def current_schema(self) -> Optional[str]:
        return await self.query_one(str, "SELECT CURRENT_SCHEMA();")

    async def create_schema(self, namespace: LocalId) -> None:
        LOGGER.debug(f"create schema: {namespace}")
        await self.execute(f"CREATE SCHEMA IF NOT EXISTS {namespace};")

    async def drop_schema(self, namespace: LocalId) -> None:
        LOGGER.debug(f"drop schema: {namespace}")
        await self.execute(f"DROP SCHEMA IF EXISTS {namespace} CASCADE;")

    def get_table(self, table: type[DataclassInstance]) -> Table:
        return self.connection.generator.state.get_table(
            self.connection.generator.get_qualified_id(table)
        )

    async def create_objects(self, tables: list[type[DataclassInstance]]) -> None:
        "Creates tables, structs, enumerations, constraints, etc. corresponding to entity class definitions."

        generator = self.connection.generator
        statement = generator.create(tables=tables)
        if statement:
            await self.execute(statement)

    async def drop_objects(self) -> None:
        "Drops all objects currently synced with the generator. (This function is mainly for unit tests.)"

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

    async def insert_rows(
        self,
        table: Table,
        records: Iterable[tuple[Any, ...]],
        *,
        field_types: tuple[type, ...],
        field_names: Optional[tuple[str, ...]] = None,
    ) -> None:
        """
        Inserts rows in a database table, ignoring duplicate keys.

        :param table: The table to insert into.
        :param field_types: The data types of the items in a row record.
        :param field_names: The field names of the items in a row record.
        :param records: The rows to be inserted into the database table.
        """

        record_generator = await self._generate_records(
            table, records, field_types=field_types, field_names=field_names
        )
        statement = self.connection.generator.get_table_insert_stmt(table)
        await self.execute_all(
            statement,
            record_generator,
        )

    async def upsert_rows(
        self,
        table: Table,
        records: Iterable[tuple[Any, ...]],
        *,
        field_types: tuple[type, ...],
        field_names: Optional[tuple[str, ...]] = None,
    ) -> None:
        """
        Inserts or updates rows in a database table.

        :param table: The table to be updated.
        :param field_types: The data types of the items in a row record.
        :param field_names: The field names of the items in a row record.
        :param records: The rows to be inserted into or updated in the database table.
        """

        record_generator = await self._generate_records(
            table, records, field_types=field_types, field_names=field_names
        )
        statement = self.connection.generator.get_table_upsert_stmt(table)
        await self.execute_all(
            statement,
            record_generator,
        )

    async def _generate_records(
        self,
        table: Table,
        records: Iterable[tuple[Any, ...]],
        *,
        field_types: tuple[type, ...],
        field_names: Optional[tuple[str, ...]] = None,
    ) -> Iterable[tuple[Any, ...]]:
        """
        Creates a record generator for a database table.

        Pass a list of field names when the order of fields in the source data does not match the column order
        in the target table.

        :param table: The database table descriptor.
        :param records: The records to insert or update.
        :param field_types: Type of each record field.
        :param field_names: Label for each record field.
        """

        if field_names is not None:
            columns: list[Column] = []

            for field_name in field_names:
                column = table.columns.get(field_name, None)
                if column is None:
                    raise ValueError(
                        f"column {LocalId(field_name)} not found in table {table.name}"
                    )
                columns.append(column)
        else:
            columns = list(table.columns.values())

        generator = self.connection.generator

        transformers: list[Optional[Callable[[Any], Any]]] = []
        for index, column, field_type in zip(range(len(columns)), columns, field_types):
            transformer = generator.get_value_transformer(column, field_type)
            if field_type is str and table.is_relation(column.name):
                relation = generator.state.get_referenced_table(table.name, column.name)
                if relation.is_lookup_table():
                    values = set(record[index] for record in records)
                    values.discard(None)  # do not insert NULL into referenced table
                    enum_dict: dict[str, int] = await self._merge_lookup_table(
                        relation, values
                    )
                    transformer = generator.get_enum_transformer(enum_dict)

            transformers.append(transformer)

        if all(transformer is None for transformer in transformers):
            return records
        else:
            return (
                tuple(
                    (
                        (transformer(field) if field is not None else None)
                        if transformer is not None
                        else field
                    )
                    for transformer, field in zip(transformers, record)
                )
                for record in records
            )

    async def _merge_lookup_table(
        self, table: Table, values: set[str]
    ) -> dict[str, int]:
        "Merges new values into a lookup table and returns the entire updated table."

        await self.execute_all(
            self.connection.generator.get_table_merge_stmt(table),
            list((value,) for value in values),
        )
        column_names = ", ".join(  # join on one element
            str(column.name) for column in table.get_value_columns()
        )
        LOGGER.debug("adding new enumeration values: {}")
        results = await self.query_all(
            tuple[str, int],
            f"SELECT {column_names}, {table.get_primary_column().name} FROM {table.name}",
        )
        return dict(results)  # type: ignore


class DiscoveryError(RuntimeError):
    pass


class Explorer(abc.ABC):
    conn: BaseContext

    def __init__(self, conn: BaseContext) -> None:
        self.conn = conn

    def get_qualified_id(self, namespace: str, id: str) -> SupportsQualifiedId:
        return QualifiedId(namespace, id)

    @abc.abstractmethod
    async def get_table_names(self) -> list[QualifiedId]:
        ...

    @abc.abstractmethod
    async def has_table(self, table_id: SupportsQualifiedId) -> bool:
        ...

    @abc.abstractmethod
    async def has_column(
        self, table_id: SupportsQualifiedId, column_id: LocalId
    ) -> bool:
        ...

    @abc.abstractmethod
    async def get_table_meta(self, table_id: SupportsQualifiedId) -> Table:
        ...

    @abc.abstractmethod
    async def get_namespace_meta(self, namespace_id: LocalId) -> Namespace:
        ...

    async def get_catalog_meta(self, *, namespace: str) -> Catalog:
        "Discovers database objects in a namespace."

        ns_name = self.conn.connection.generator.converter.options.namespaces.get(
            namespace
        )
        if ns_name is None:
            raise DiscoveryError(
                "discovery expects a (pseudo) namespace but options map to blank name"
            )

        ns = await self.get_namespace_meta(LocalId(ns_name))
        return Catalog(namespaces=[ns])

    async def synchronize(self, module: types.ModuleType) -> None:
        "Synchronizes a current source state with a desired target state."

        generator = self.conn.connection.generator

        # determine target database schema
        generator.reset()
        generator.create(tables=get_entity_types([module]))
        target_state = generator.state

        # acquire current database schema
        generator.state = await self.get_catalog_meta(namespace=module.__name__)

        # mutate current state into desired state
        stmt = generator.get_mutate_stmt(target_state)
        if stmt is not None:
            LOGGER.info(f"synchronize schema with SQL:\n{stmt}")
            await self.conn.execute(stmt)
            generator.state = target_state


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
        "Instantiates an explorer that can discover objects in a database."

        explorer_type = self.get_explorer_type()
        return explorer_type(conn)
