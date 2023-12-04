"""
pysqlsync: Synchronize schema and large volumes of data.

This module defines base classes to create a connection, generate SQL, discover database objects, and synchronize
schema and data.

:see: https://github.com/hunyadi/pysqlsync
"""

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
from .formation.mutation import Mutator, MutatorOptions
from .formation.object_types import Catalog, Column, Namespace, ObjectFactory, Table
from .formation.py_to_sql import ArrayMode, DataclassConverter, EnumMode, StructMode
from .model.data_types import SqlJsonType, SqlVariableCharacterType
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
    :param synchronization: Synchronization options.
    :param skip_annotations: Annotation classes to ignore on table column types.
    """

    enum_mode: Optional[EnumMode] = None
    struct_mode: Optional[StructMode] = None
    array_mode: Optional[ArrayMode] = None
    namespaces: dict[types.ModuleType, Optional[str]] = dataclasses.field(
        default_factory=dict
    )
    foreign_constraints: bool = True
    synchronization: MutatorOptions = dataclasses.field(default_factory=MutatorOptions)
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
    mutator: Mutator
    converter: DataclassConverter
    state: Catalog

    def __init__(
        self,
        options: GeneratorOptions,
        factory: Optional[ObjectFactory] = None,
        mutator: Optional[Mutator] = None,
    ) -> None:
        self.options = options
        self.factory = factory if factory is not None else ObjectFactory()
        self.mutator = mutator if mutator is not None else Mutator()
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

        if tables is not None:
            if not tables:
                LOGGER.warning("no tables to create")

            target = self.converter.dataclasses_to_catalog(tables)
            statement = self.get_mutate_stmt(target)
            self.state = target
            return statement
        elif modules is not None:
            if not modules:
                LOGGER.warning("no schemas to create")

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

        return self.mutator.mutate_catalog_stmt(self.state, target)

    def get_table_insert_stmt(
        self, table: Table, field_names: Optional[tuple[str, ...]] = None
    ) -> str:
        """
        Returns a SQL statement to insert new records in a database table.

        This statement omits values for identity columns.
        """

        return self.get_table_merge_stmt(table, field_names)

    @abc.abstractmethod
    def get_table_merge_stmt(
        self, table: Table, field_names: Optional[tuple[str, ...]] = None
    ) -> str:
        """
        Returns a SQL statement to insert or ignore records in a database table.

        This statement omits values for identity columns.
        """

        ...

    @abc.abstractmethod
    def get_table_upsert_stmt(
        self, table: Table, field_names: Optional[tuple[str, ...]] = None
    ) -> str:
        """
        Returns a SQL statement to insert or update records in a database table.

        This statement uses identity column values for lookup.
        """

        ...

    @abc.abstractmethod
    def get_table_delete_stmt(self, table: Table) -> str:
        "Returns a SQL statement to delete rows from a database table."

        ...

    def get_qualified_id(self, table: type[DataclassInstance]) -> SupportsQualifiedId:
        return self.converter.create_qualified_id(table.__module__, table.__name__)

    def get_current_schema_stmt(self) -> str:
        return "CURRENT_SCHEMA()"

    def get_dataclass_insert_stmt(self, table: type[DataclassInstance]) -> str:
        "Returns a SQL statement to insert or ignore records in a database table."

        table_object = self.state.get_table(self.get_qualified_id(table))
        return self.get_table_insert_stmt(table_object)

    def get_dataclass_upsert_stmt(self, table: type[DataclassInstance]) -> str:
        "Returns a SQL statement to insert or update records in a database table."

        table_object = self.state.get_table(self.get_qualified_id(table))
        return self.get_table_upsert_stmt(table_object)

    def get_dataclass_delete_stmt(self, table: type[DataclassInstance]) -> str:
        "Returns a SQL statement to delete records from a database table."

        table_object = self.state.get_table(self.get_qualified_id(table))
        return self.get_table_delete_stmt(table_object)

    def get_dataclass_as_record(
        self, entity_type: type[D], item: D, *, skip_identity: bool = False
    ) -> tuple:
        "Converts a data-class object into a record to insert into a database table."

        table_object = self.state.get_table(self.get_qualified_id(entity_type))
        extractors = self._get_dataclass_extractors(
            table_object, entity_type, skip_identity=skip_identity
        )
        return tuple(extractor(item) for extractor in extractors)

    def get_dataclasses_as_records(
        self,
        entity_type: type[D],
        items: Iterable[D],
        *,
        skip_identity: bool = False,
    ) -> Iterable[tuple]:
        "Converts a list of data-class objects into a list of records to insert into a database table."

        table_object = self.state.get_table(self.get_qualified_id(entity_type))
        extractors = self._get_dataclass_extractors(
            table_object, entity_type, skip_identity=skip_identity
        )
        return (tuple(extractor(item) for extractor in extractors) for item in items)

    def _get_dataclass_extractors(
        self, table: Table, entity_type: type[DataclassInstance], *, skip_identity: bool
    ) -> tuple[Callable[[Any], Any], ...]:
        "Returns a tuple of callable function objects that extracts each field of a data-class."

        return tuple(
            self.get_field_extractor(table.columns[field.name], field.name, field.type)
            for field in dataclasses.fields(entity_type)
            if not (skip_identity and table.columns[field.name].identity)
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

        if isinstance(column.data_type, SqlJsonType) or (
            isinstance(column.data_type, SqlVariableCharacterType)
            and (field_type is dict or field_type is list)
        ):
            return lambda field: _JSON_ENCODER.encode(field)

        if is_type_enum(field_type):
            return lambda field: field.value
        else:
            return None

    def get_enum_transformer(self, enum_dict: dict[str, int]) -> Callable[[Any], Any]:
        "Returns a callable function object that looks up an assigned integer value based on the enumeration string value."

        return lambda field: enum_dict[field]

    def get_enum_list_transformer(
        self, enum_dict: dict[str, int]
    ) -> Callable[[Any], Any]:
        "Returns a callable function object that looks up assigned integer values based on a list of enumeration string values."

        return lambda field: [enum_dict[item] for item in field]


class QueryException(RuntimeError):
    query: str

    def __init__(self, query: str) -> None:
        super().__init__()
        self.query = query

    def __str__(self) -> str:
        query = f"{self.query[:1000]}..." if len(self.query) > 1000 else self.query
        return f"error executing query:\n{query}"


@dataclass
class ConnectionParameters:
    "Database connection parameters that would typically be encapsulated in a connection string."

    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None

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

    async def __aenter__(self) -> "BaseContext":
        return await self.open()

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> None:
        await self.close()

    @abc.abstractmethod
    async def open(self) -> "BaseContext":
        ...

    @abc.abstractmethod
    async def close(self) -> None:
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
            if not len(args):
                LOGGER.warning("no data to execute statement with")
                return
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
        func = self.connection.generator.get_current_schema_stmt()
        return await self.query_one(str, f"SELECT {func};")

    async def create_schema(self, namespace: LocalId) -> None:
        LOGGER.debug(f"create schema: {namespace}")
        factory = self.connection.generator.factory
        stmt = factory.namespace_class(namespace).create_schema_stmt()
        if stmt:
            await self.execute(stmt)

    async def drop_schema(self, namespace: LocalId) -> None:
        LOGGER.debug(f"drop schema: {namespace}")
        factory = self.connection.generator.factory
        stmt = factory.namespace_class(namespace).drop_schema_stmt()
        if stmt:
            await self.execute(stmt)

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

        if isinstance(data, Sized) and not len(data):
            LOGGER.warning("no data to insert")
            return

        generator = self.connection.generator
        statement = generator.get_dataclass_insert_stmt(table)
        records = generator.get_dataclasses_as_records(table, data, skip_identity=True)
        await self.execute_all(statement, records)

    async def upsert_data(self, table: type[D], data: Iterable[D]) -> None:
        "Inserts or updates data in the database table corresponding to the dataclass type."

        if isinstance(data, Sized) and not len(data):
            LOGGER.warning("no data to upsert")
            return

        generator = self.connection.generator
        statement = generator.get_dataclass_upsert_stmt(table)
        records = generator.get_dataclasses_as_records(table, data, skip_identity=False)
        await self.execute_all(statement, records)

    async def delete_data(
        self, table: type[DataclassInstance], keys: Iterable[Any]
    ) -> None:
        "Inserts or updates data in the database table corresponding to the dataclass type."

        if isinstance(keys, Sized) and not len(keys):
            LOGGER.warning("no data to delete")
            return

        generator = self.connection.generator
        statement = generator.get_dataclass_delete_stmt(table)
        await self.execute_all(statement, ((key,) for key in keys))

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

        if isinstance(records, Sized):
            LOGGER.debug(f"insert {len(records)} rows into {table.name}")
            if not len(records):
                LOGGER.warning("no rows to insert")
                return
        else:
            LOGGER.debug(f"insert into {table.name}")

        await self._insert_rows(
            table, records, field_types=field_types, field_names=field_names
        )

        if isinstance(records, Sized):
            LOGGER.info(f"{len(records)} rows have been inserted into {table.name}")

    async def _insert_rows(
        self,
        table: Table,
        records: Iterable[tuple[Any, ...]],
        *,
        field_types: tuple[type, ...],
        field_names: Optional[tuple[str, ...]] = None,
    ) -> None:
        """
        Inserts rows in a database table, ignoring duplicate keys.

        Override in engine-specific derived classes.
        """

        record_generator = await self._generate_records(
            table, records, field_types=field_types, field_names=field_names
        )
        order = tuple(name for name in field_names if name) if field_names else None
        statement = self.connection.generator.get_table_insert_stmt(table, order)
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

        if isinstance(records, Sized):
            LOGGER.debug(f"upsert {len(records)} rows into {table.name}")
            if not len(records):
                LOGGER.warning("no rows to upsert")
                return
        else:
            LOGGER.debug(f"upsert into {table.name}")

        await self._upsert_rows(
            table, records, field_types=field_types, field_names=field_names
        )

        if isinstance(records, Sized):
            LOGGER.info(
                f"{len(records)} rows have been inserted or updated into {table.name}"
            )

    async def _upsert_rows(
        self,
        table: Table,
        records: Iterable[tuple[Any, ...]],
        *,
        field_types: tuple[type, ...],
        field_names: Optional[tuple[str, ...]] = None,
    ) -> None:
        """
        Inserts or updates rows in a database table.

        Override in engine-specific derived classes.
        """

        record_generator = await self._generate_records(
            table, records, field_types=field_types, field_names=field_names
        )
        order = tuple(name for name in field_names if name) if field_names else None
        statement = self.connection.generator.get_table_upsert_stmt(table, order)
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
        :param field_types: Type of each record field. Use `types.NoneType` to omit a field.
        :param field_names: Label for each record field. Use an empty string for an omitted field.
        """

        generator = self.connection.generator

        if field_names is None:
            field_names = tuple(name for name in table.columns.keys())

        indices: list[int] = []
        transformers: list[Optional[Callable[[Any], Any]]] = []
        for index, field_type, field_name in zip(
            range(len(field_types)), field_types, field_names
        ):
            if field_type is types.NoneType or not field_name:
                continue

            indices.append(index)
            transformer = await self._get_transformer(
                table, records, generator, index, field_type, field_name
            )
            transformers.append(transformer)

        if all(transformer is None for transformer in transformers):
            if len(indices) == len(field_types):
                return records
            else:
                return (tuple(record[i] for i in indices) for record in records)
        else:
            if len(indices) == len(field_types):
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
            else:
                return (
                    tuple(
                        (
                            (transformer(field) if field is not None else None)
                            if transformer is not None
                            else field
                        )
                        for transformer, field in zip(
                            transformers, (record[i] for i in indices)
                        )
                    )
                    for record in records
                )

    async def _get_transformer(
        self,
        table: Table,
        records: Iterable[tuple[Any, ...]],
        generator: BaseGenerator,
        index: int,
        field_type: type,
        field_name: str,
    ) -> Optional[Callable[[Any], Any]]:
        """
        Creates a transformer for a record field.

        :param table: The database table descriptor.
        :param records: The records to insert or update.
        :param generator: The generator that produces the transformer function.
        :param index: Field index.
        :param field_type: Type of field. Use `types.NoneType` to omit a field.
        :param field_name: Label for field. Use an empty string for an omitted field.
        """

        column = table.columns.get(field_name)
        if column is None:
            raise ValueError(
                f"column {LocalId(field_name)} not found in table {table.name}"
            )

        transformer = generator.get_value_transformer(column, field_type)

        if table.is_relation(column):
            relation = generator.state.get_referenced_table(table.name, column.name)
            if relation.is_lookup_table():
                enum_dict: dict[str, int]

                if field_type is str:
                    # a single enumeration value represented as a string
                    values = set(record[index] for record in records)
                    values.discard(None)  # do not insert NULL into referenced table
                    enum_dict = await self._merge_lookup_table(relation, values)
                    return generator.get_enum_transformer(enum_dict)
                elif field_type is list:
                    # a list of enumeration values represented as a list of strings
                    values = set()
                    for record in records:
                        values.update(record[index])
                    values.discard(None)  # do not insert NULL into referenced table
                    enum_dict = await self._merge_lookup_table(relation, values)
                    return generator.get_enum_list_transformer(enum_dict)

        return transformer

    async def _merge_lookup_table(
        self, table: Table, values: set[str]
    ) -> dict[str, int]:
        "Merges new values into a lookup table and returns the entire updated table."

        if not values:
            return dict()

        await self.execute_all(
            self.connection.generator.get_table_merge_stmt(table),
            list((value,) for value in values),
        )

        value_name = table.get_value_columns()[0].name
        index_name = table.get_primary_column().name
        LOGGER.debug("adding new enumeration values: {}")
        results = await self.query_all(
            tuple[str, int],
            f"SELECT {value_name}, {index_name} FROM {table.name}",
        )
        return dict(results)  # type: ignore

    async def delete_rows(
        self, table: Table, key_type: type, key_values: Iterable[Any]
    ) -> None:
        """
        Deletes rows from a database table.

        :param table: The table to remove records from.
        :param key_type: The data type of the key values.
        :param key_values: The key values to look up in the table.
        """

        if isinstance(key_values, Sized):
            LOGGER.debug(f"delete {len(key_values)} rows from {table.name}")
            if not len(key_values):
                LOGGER.warning("no rows to delete")
                return
        else:
            LOGGER.debug(f"delete from {table.name}")

        await self._delete_rows(table, key_type, key_values)

        if isinstance(key_values, Sized):
            LOGGER.info(f"{len(key_values)} rows have been deleted from {table.name}")

    async def _delete_rows(
        self, table: Table, key_type: type, key_values: Iterable[Any]
    ) -> None:
        """
        Deletes rows from a database table.

        Override in engine-specific derived classes.
        """

        generator = self.connection.generator
        transformer = generator.get_value_transformer(
            table.get_primary_column(), key_type
        )
        if transformer is not None:
            records = ((transformer(key),) for key in key_values)
        else:
            records = ((key,) for key in key_values)

        statement = generator.get_table_delete_stmt(table)
        await self.execute_all(statement, records)


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
    async def get_table(self, table_id: SupportsQualifiedId) -> Table:
        ...

    @abc.abstractmethod
    async def get_namespace(self, namespace_id: LocalId) -> Namespace:
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

        ns = await self.get_namespace(LocalId(ns_name))
        return Catalog(namespaces=[ns])

    async def acquire(self, *, namespace: str) -> None:
        "Merges database objects in a namespace into the current model."

        generator = self.conn.connection.generator
        generator.state.merge(await self.get_catalog_meta(namespace=namespace))

    @overload
    async def synchronize(self, *, module: types.ModuleType) -> None:
        ...

    @overload
    async def synchronize(self, *, modules: list[types.ModuleType]) -> None:
        ...

    async def synchronize(
        self,
        *,
        module: Optional[types.ModuleType] = None,
        modules: Optional[list[types.ModuleType]] = None,
    ) -> None:
        "Synchronizes a current source state with a desired target state."

        if module is None and modules is None:
            raise TypeError("required: one of parameters `module` and `modules`")
        if module is not None and modules is not None:
            raise TypeError("disallowed: both parameters `module` and `modules`")

        if modules is not None:
            entity_modules = modules
        elif module is not None:
            entity_modules = [module]
        else:
            raise NotImplementedError()

        generator = self.conn.connection.generator

        # determine target database schema
        generator.reset()
        generator.create(tables=get_entity_types(entity_modules))
        target_state = generator.state

        # acquire current database schema
        generator.reset()
        for m in entity_modules:
            await self.acquire(namespace=m.__name__)
        LOGGER.debug(f"found {len(generator.state.namespaces)} namespaces")
        for ns in generator.state.namespaces.values():
            LOGGER.debug(f"found {len(ns.enums)} enums in namespace {ns.name}")
            LOGGER.debug(f"found {len(ns.structs)} structs in namespace {ns.name}")
            LOGGER.debug(f"found {len(ns.tables)} tables in namespace {ns.name}")

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

    def create_generator(
        self, options: Optional[GeneratorOptions] = None
    ) -> BaseGenerator:
        "Instantiates a generator that can emit SQL statements."

        generator_options = options if options is not None else GeneratorOptions()
        generator_type = self.get_generator_type()
        return generator_type(generator_options)

    def create_explorer(self, conn: BaseContext) -> Explorer:
        "Instantiates an explorer that can discover objects in a database."

        explorer_type = self.get_explorer_type()
        return explorer_type(conn)
