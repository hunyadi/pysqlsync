"""
pysqlsync: Synchronize schema and large volumes of data.

This module defines base classes to create a connection, generate SQL, discover database objects, and synchronize
schema and data.

:see: https://github.com/hunyadi/pysqlsync
"""

import abc
import dataclasses
import enum
import json
import logging
import types
import typing
from dataclasses import dataclass
from typing import (
    Any,
    AsyncIterable,
    Callable,
    Iterable,
    Optional,
    Sized,
    TypeVar,
    Union,
    overload,
)

from strong_typing.inspection import DataclassInstance, is_dataclass_type, is_type_enum
from strong_typing.name import python_type_to_str

from .connection import ConnectionParameters
from .formation.inspection import get_entity_types
from .formation.mutation import Mutator, MutatorOptions
from .formation.object_types import (
    Catalog,
    Column,
    FormationError,
    Namespace,
    ObjectFactory,
    Table,
)
from .formation.py_to_sql import ArrayMode, DataclassConverter, EnumMode, StructMode
from .model.data_types import SqlJsonType, SqlVariableCharacterType
from .model.id_types import LocalId, QualifiedId, SupportsQualifiedId
from .python_types import dataclass_to_code, module_to_code

D = TypeVar("D", bound=DataclassInstance)
E = TypeVar("E", bound=enum.Enum)
T = TypeVar("T")

RecordType = tuple[Any, ...]
RecordSource = Union[Iterable[RecordType], AsyncIterable[RecordType]]

LOGGER = logging.getLogger("pysqlsync")

_JSON_ENCODER = json.JSONEncoder(
    ensure_ascii=False,
    check_circular=False,
    allow_nan=False,
    indent=None,
    separators=(",", ":"),
)

# number of records consumed from an asynchronous iterator before a batch is dispatched to a database
BATCH_SIZE = 100000


class ClassRef:
    "Represents a reference to a Python class in a Python module."

    module_name: str
    entity_name: str

    @overload
    def __init__(self, entity_type: type[DataclassInstance]) -> None:
        """
        Creates a reference from a Python data-class type.

        :param entity_type: A Python data-class type.
        """
        ...

    @overload
    def __init__(self, *, module: types.ModuleType, entity_name: str) -> None:
        """
        Creates a reference from a Python class name in a Python module.

        :param module: A Python module instance.
        :param entity_name: Class name without module qualifier.
        """
        ...

    @overload
    def __init__(self, *, module_name: str, entity_name: str) -> None:
        """
        Creates a reference from a Python fully-qualified module name and Python class name pair.

        :param module_name: Fully-qualified module name.
        :param entity_name: Class name without module qualifier.
        """
        ...

    def __init__(
        self,
        entity_type: Optional[type[DataclassInstance]] = None,
        *,
        module: Optional[types.ModuleType] = None,
        module_name: Optional[str] = None,
        entity_name: Optional[str] = None,
    ) -> None:
        if module_name is None:
            if module is not None:
                module_name = module.__name__
            elif entity_type is not None:
                module_name = entity_type.__module__
            else:
                raise ValueError("expected: module name or type")

        if entity_name is None:
            if entity_type is not None:
                entity_name = entity_type.__name__
            else:
                raise ValueError("expected: entity class name or type")

        self.module_name = module_name
        self.entity_name = entity_name


@dataclass
class GeneratorOptions:
    """
    Database-agnostic generator options.

    :param enum_mode: Conversion mode for enumeration types.
    :param struct_mode: Conversion mode for composite types.
    :param array_mode: Conversion mode for sequence types.
    :param namespaces: Maps Python modules into SQL namespaces (a.k.a. schemas).
    :param foreign_constraints: Whether to create foreign/primary key relationships between tables.
    :param initialize_tables: Whether to populate special tables (e.g. enumerations) with data.
    :param synchronization: Synchronization options.
    :param skip_annotations: Annotation classes to ignore on table column types.
    :param auto_default: Automatically assign a default value to non-nullable types.
    """

    enum_mode: Optional[EnumMode] = None
    struct_mode: Optional[StructMode] = None
    array_mode: Optional[ArrayMode] = None
    namespaces: dict[types.ModuleType, Optional[str]] = dataclasses.field(
        default_factory=dict
    )
    foreign_constraints: bool = True
    initialize_tables: bool = False
    synchronization: MutatorOptions = dataclasses.field(default_factory=MutatorOptions)
    skip_annotations: tuple[type, ...] = ()
    auto_default: bool = False


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

    def _check_mode(
        self,
        mode: Optional[E],
        *,
        matches: Optional[E] = None,
        include: Optional[list[E]] = None,
        exclude: Optional[list[E]] = None,
    ) -> None:
        if mode is None:
            return

        fail: bool = False
        if matches is not None:
            if mode is not matches:
                fail = True
        if include is not None:
            if mode not in include:
                fail = True
        if exclude is not None:
            if mode in exclude:
                fail = True
        if fail:
            raise FormationError(
                f"unsupported {mode.__class__.__name__} for {self.__class__.__name__}: {mode}"
            )

    def check_enum_mode(
        self,
        *,
        matches: Optional[EnumMode] = None,
        include: Optional[list[EnumMode]] = None,
        exclude: Optional[list[EnumMode]] = None,
    ) -> None:
        self._check_mode(
            self.options.enum_mode, matches=matches, include=include, exclude=exclude
        )

    def check_struct_mode(
        self,
        *,
        matches: Optional[StructMode] = None,
        include: Optional[list[StructMode]] = None,
        exclude: Optional[list[StructMode]] = None,
    ) -> None:
        self._check_mode(
            self.options.struct_mode, matches=matches, include=include, exclude=exclude
        )

    def check_array_mode(
        self,
        *,
        matches: Optional[ArrayMode] = None,
        include: Optional[list[ArrayMode]] = None,
        exclude: Optional[list[ArrayMode]] = None,
    ) -> None:
        self._check_mode(
            self.options.array_mode, matches=matches, include=include, exclude=exclude
        )

    @overload
    def create(self, *, tables: list[type[DataclassInstance]]) -> Optional[str]: ...

    @overload
    def create(self, *, modules: list[types.ModuleType]) -> Optional[str]: ...

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

            if LOGGER.isEnabledFor(logging.DEBUG):
                for table in tables:
                    code = dataclass_to_code(table)
                    LOGGER.debug("analyzing dataclass `%s`:\n%s", table.__name__, code)

            target = self.converter.dataclasses_to_catalog(tables)
            statement = self.get_mutate_stmt(target)
            self.state = target
            return statement
        elif modules is not None:
            if not modules:
                LOGGER.warning("no schemas to create")

            if LOGGER.isEnabledFor(logging.DEBUG):
                for module in modules:
                    code = module_to_code(module)
                    LOGGER.debug("analyzing module `%s`:\n%s", module.__name__, code)

            target = self.converter.modules_to_catalog(modules)
            statement = self.get_mutate_stmt(target)
            self.state = target
            return statement
        else:
            # should never be triggered; either `tables` or `modules` must be defined at this point
            raise NotImplementedError("match condition not exhaustive")

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

    @abc.abstractmethod
    def placeholder(self, index: int) -> str:
        """
        Returns a placeholder for a positional argument in a prepared statement.

        :param index: An index starting at 1 for the first position.
        """
        ...

    def get_table_insert_stmt(
        self, table: Table, order: Optional[tuple[str, ...]] = None
    ) -> str:
        """
        Returns a SQL statement to insert new records in a database table.

        This statement omits values for identity columns.
        """

        statements: list[str] = []
        statements.append(f"INSERT INTO {table.name}")
        columns = [column for column in table.get_columns(order) if not column.identity]
        column_list = ", ".join(str(column.name) for column in columns)
        value_list = ", ".join(
            self.placeholder(index) for index, _ in enumerate(columns, start=1)
        )
        statements.append(f"({column_list}) VALUES ({value_list})")
        statements.append(";")
        return "\n".join(statements)

    def _get_merge_preamble(self, table: Table, columns: list[Column]) -> list[str]:
        "Builds the match condition of a SQL MERGE statement."

        statements: list[str] = []

        statements.append(f"MERGE INTO {table.name} target")
        column_list = ", ".join(str(column.name) for column in columns)
        value_list = ", ".join(
            self.placeholder(index) for index, _ in enumerate(columns, start=1)
        )
        statements.append(f"USING (VALUES ({value_list})) source({column_list})")

        match_columns = [column for column in columns if table.is_lookup_column(column)]
        if match_columns:
            match_condition = " OR ".join(
                f"target.{column.name} = source.{column.name}"
                for column in match_columns
            )
            statements.append(f"ON ({match_condition})")

        return statements

    def get_table_merge_stmt(
        self, table: Table, order: Optional[tuple[str, ...]] = None
    ) -> str:
        """
        Returns a SQL statement to insert or ignore records in a database table.

        This statement omits values for identity columns.
        """

        columns = [column for column in table.get_columns(order) if not column.identity]
        statements = self._get_merge_preamble(table, columns)

        statements.append("WHEN NOT MATCHED THEN")
        column_list = ", ".join(str(column.name) for column in columns)
        insert_list = ", ".join(f"source.{column.name}" for column in columns)
        statements.append(f"INSERT ({column_list}) VALUES ({insert_list})")

        statements.append(";")
        return "\n".join(statements)

    def get_table_upsert_stmt(
        self, table: Table, order: Optional[tuple[str, ...]] = None
    ) -> str:
        """
        Returns a SQL statement to insert or update records in a database table.

        This statement uses identity column values for lookup.
        """

        columns = [column for column in table.get_columns(order)]
        statements: list[str] = self._get_merge_preamble(table, columns)

        insert_columns = [column for column in columns if not column.identity]
        update_columns = [
            column for column in insert_columns if not table.is_lookup_column(column)
        ]
        if update_columns:
            statements.append("WHEN MATCHED THEN")
            update_list = ", ".join(
                f"target.{column.name} = source.{column.name}"
                for column in update_columns
            )
            statements.append(f"UPDATE SET {update_list}")
        if insert_columns:
            statements.append("WHEN NOT MATCHED THEN")
            column_list = ", ".join(str(column.name) for column in insert_columns)
            insert_list = ", ".join(
                f"source.{column.name}" for column in insert_columns
            )
            statements.append(f"INSERT ({column_list}) VALUES ({insert_list})")

        statements.append(";")
        return "\n".join(statements)

    def get_table_delete_stmt(self, table: Table) -> str:
        "Returns a SQL statement to delete rows from a database table."

        return f"DELETE FROM {table.name} WHERE {table.get_primary_column().name} = {self.placeholder(1)}"

    def get_qualified_id(self, ref: ClassRef) -> SupportsQualifiedId:
        return self.converter.create_qualified_id(ref.module_name, ref.entity_name)

    def get_current_schema_stmt(self) -> str:
        return "CURRENT_SCHEMA()"

    def get_dataclass_insert_stmt(self, table: type[DataclassInstance]) -> str:
        "Returns a SQL statement to insert or ignore records in a database table."

        table_object = self.state.get_table(self.get_qualified_id(ClassRef(table)))
        return self.get_table_insert_stmt(table_object)

    def get_dataclass_upsert_stmt(self, table: type[DataclassInstance]) -> str:
        "Returns a SQL statement to insert or update records in a database table."

        table_object = self.state.get_table(self.get_qualified_id(ClassRef(table)))
        return self.get_table_upsert_stmt(table_object)

    def get_dataclass_delete_stmt(self, table: type[DataclassInstance]) -> str:
        "Returns a SQL statement to delete records from a database table."

        table_object = self.state.get_table(self.get_qualified_id(ClassRef(table)))
        return self.get_table_delete_stmt(table_object)

    def get_dataclass_as_record(
        self, entity_type: type[D], item: D, *, skip_identity: bool = False
    ) -> tuple:
        "Converts a data-class object into a record to insert into a database table."

        table_object = self.state.get_table(
            self.get_qualified_id(ClassRef(entity_type))
        )
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

        table_object = self.state.get_table(
            self.get_qualified_id(ClassRef(entity_type))
        )
        extractors = self._get_dataclass_extractors(
            table_object, entity_type, skip_identity=skip_identity
        )
        return (tuple(extractor(item) for extractor in extractors) for item in items)

    def _get_dataclass_extractors(
        self, table: Table, entity_type: type[DataclassInstance], *, skip_identity: bool
    ) -> tuple[Callable[[Any], Any], ...]:
        "Returns a tuple of callable function objects that extracts each field of a data-class."

        return tuple(
            self.get_field_extractor(
                table.columns[field.name], field.name, typing.cast(type, field.type)
            )
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
            and (field_type is dict or field_type is list or field_type is set)
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
    async def open(self) -> "BaseContext": ...

    @abc.abstractmethod
    async def close(self) -> None: ...


class RecordTransformer:
    @abc.abstractmethod
    async def get(
        self, records: Iterable[RecordType]
    ) -> Optional[Callable[[Any], Any]]:
        """
        Returns a callable function object that performs a transformation on a value.

        :param records: Records in a batch to insert/update. Used for context.
        """

        ...


class ScalarTransformer(RecordTransformer):
    "A context-free transformer that mutates a scalar value into another."

    fn: Optional[Callable[[Any], Any]]

    def __init__(
        self,
        fn: Optional[Callable[[Any], Any]],
    ) -> None:
        self.fn = fn

    async def get(
        self, records: Iterable[RecordType]
    ) -> Optional[Callable[[Any], Any]]:
        return self.fn


class BaseEnumTransformer(RecordTransformer):
    context: "BaseContext"
    table: Table
    generator: BaseGenerator
    index: int

    def __init__(
        self,
        context: "BaseContext",
        table: Table,
        generator: BaseGenerator,
        index: int,
    ) -> None:
        self.context = context
        self.table = table
        self.generator = generator
        self.index = index

    async def _merge_lookup_table(self, values: set[str]) -> dict[str, int]:
        "Merges new values into a lookup table and returns the entire updated table."

        if not values:
            return dict()

        await self.context.execute_all(
            self.context.connection.generator.get_table_merge_stmt(self.table),
            list((value,) for value in values),
        )

        value_name = self.table.get_value_columns()[0].name
        index_name = self.table.get_primary_column().name
        LOGGER.debug("adding new enumeration values: %s", values)
        results = await self.context.query_all(
            tuple[str, int],
            f"SELECT {value_name}, {index_name} FROM {self.table.name}",
        )
        return dict(results)


class EnumTransformer(BaseEnumTransformer):
    "A transformer that looks up an assigned integer value based on the enumeration string value."

    async def get(
        self, records: Iterable[RecordType]
    ) -> Optional[Callable[[Any], Any]]:
        "Returns a callable function object that looks up an assigned integer value based on the enumeration string value."

        # a single enumeration value represented as a string
        values: set[Optional[str]] = set()
        for record in records:
            values.add(record[self.index])
        values.discard(None)  # do not insert NULL into referenced table
        enum_dict = await self._merge_lookup_table(typing.cast(set[str], values))
        return self.generator.get_enum_transformer(enum_dict)


class EnumListTransformer(BaseEnumTransformer):
    "A transformer that looks up assigned integer values based on a list of enumeration string values."

    async def get(
        self, records: Iterable[RecordType]
    ) -> Optional[Callable[[Any], Any]]:
        "Returns a callable function object that looks up assigned integer values based on a list of enumeration string values."

        # a list of enumeration values represented as a list of strings
        values: set[Optional[str]] = set()
        for record in records:
            values.update(record[self.index])
        values.discard(None)  # do not insert NULL into referenced table
        enum_list_dict = await self._merge_lookup_table(typing.cast(set[str], values))
        return self.generator.get_enum_list_transformer(enum_list_dict)


class DataSource:
    "A data source from which records can be retrieved in batches."

    @abc.abstractmethod
    async def batches(self) -> AsyncIterable[list[RecordType]]:
        "Produces a batch of records."

        # yield required for mypy to properly identify return type signature in derived classes
        yield []


class IterableDataSource(DataSource):
    """
    A data source created from a synchronous iterable of records.

    Each element in the iterator is accessed only once.
    """

    records: Iterable[RecordType]

    def __init__(self, records: Iterable[RecordType]) -> None:
        self.records = records

    async def batches(self) -> AsyncIterable[list[RecordType]]:
        rows: list[RecordType] = []
        for record in self.records:
            rows.append(record)
            if len(rows) >= BATCH_SIZE:
                yield rows
                rows = []
        if rows:
            yield rows


class AsyncIterableDataSource(DataSource):
    """
    A data source created from an asynchronous iterable of records.

    Each element in the iterator is accessed only once.
    """

    records: AsyncIterable[RecordType]

    def __init__(self, records: AsyncIterable[RecordType]) -> None:
        self.records = records

    async def batches(self) -> AsyncIterable[list[RecordType]]:
        rows: list[RecordType] = []
        async for record in self.records:
            rows.append(record)
            if len(rows) >= BATCH_SIZE:
                yield rows
                rows = []
        if rows:
            yield rows


class CompositeDataSource(DataSource):
    "A data source created by transforming the output of another source."

    source: DataSource

    def __init__(self, source: DataSource) -> None:
        self.source = source


class SelectorDataSource(CompositeDataSource):
    "A data source derived from another by selecting specific columns."

    indices: list[int]

    def __init__(self, source: DataSource, indices: list[int]) -> None:
        super().__init__(source)
        self.indices = indices

    async def batches(self) -> AsyncIterable[list[RecordType]]:
        indices = self.indices
        async for batch in self.source.batches():
            yield [tuple(record[i] for i in indices) for record in batch]


class TransformerDataSource(CompositeDataSource):
    "A data source derived from another by transforming specific columns."

    transformers: list[RecordTransformer]

    def __init__(
        self, source: DataSource, transformers: list[RecordTransformer]
    ) -> None:
        super().__init__(source)
        self.transformers = transformers

    async def batches(self) -> AsyncIterable[list[RecordType]]:
        async for batch in self.source.batches():
            fns: list[Optional[Callable[[Any], Any]]] = []
            for transformer in self.transformers:
                fns.append(await transformer.get(batch))
            yield [
                tuple(
                    (
                        (fn(field) if field is not None else None)
                        if fn is not None
                        else field
                    )
                    for fn, field in zip(fns, record)
                )
                for record in batch
            ]


class SelectorTransformerDataSource(CompositeDataSource):
    "A data source derived from another by selecting and/or transforming specific columns."

    indices: list[int]
    transformers: list[RecordTransformer]

    def __init__(
        self,
        source: DataSource,
        indices: list[int],
        transformers: list[RecordTransformer],
    ) -> None:
        super().__init__(source)
        self.indices = indices
        self.transformers = transformers

    async def batches(self) -> AsyncIterable[list[RecordType]]:
        indices = self.indices
        async for batch in self.source.batches():
            fns: list[Optional[Callable[[Any], Any]]] = []
            for transformer in self.transformers:
                fns.append(await transformer.get(batch))
            yield [
                tuple(
                    (
                        (fn(field) if field is not None else None)
                        if fn is not None
                        else field
                    )
                    for fn, field in zip(fns, (record[i] for i in indices))
                )
                for record in batch
            ]


def to_data_source(records: RecordSource) -> DataSource:
    "Converts a synchronous or asynchronous iterable of records into a data source."

    if isinstance(records, Iterable):
        return IterableDataSource(records)
    elif isinstance(records, AsyncIterable):
        return AsyncIterableDataSource(records)
    else:
        raise TypeError("expected: `Iterable` or `AsyncIterable` of records")


class BaseContext(abc.ABC):
    "Context object returned by a connection object."

    connection: BaseConnection

    def __init__(self, connection: BaseConnection) -> None:
        self.connection = connection

    async def execute(self, statement: str) -> None:
        "Executes one or more SQL statements."

        if not statement:
            raise ValueError("empty statement")
        if not statement.strip():
            raise ValueError("blank statement")

        LOGGER.debug("execute SQL:\n%s", statement)
        try:
            await self._execute(statement)
        except QueryException:
            raise
        except Exception as e:
            raise QueryException(statement) from e

    @abc.abstractmethod
    async def _execute(self, statement: str) -> None:
        "Executes one or more SQL statements."

        ...

    async def execute_all(self, statement: str, records: RecordSource) -> None:
        "Executes a SQL statement with several records of data."

        if not statement:
            raise ValueError("empty statement")
        if not statement.strip():
            raise ValueError("blank statement")

        if isinstance(records, Sized):
            LOGGER.debug("execute SQL with %d rows:\n%s", len(records), statement)
            if not len(records):
                LOGGER.warning("no data to execute statement with")
                return
        else:
            LOGGER.debug("execute SQL:\n%s", statement)

        source = to_data_source(records)
        try:
            await self._execute_all(statement, source)
        except QueryException:
            raise
        except Exception as e:
            raise QueryException(statement) from e

    @abc.abstractmethod
    async def _execute_all(self, statement: str, source: DataSource) -> None:
        "Executes a SQL statement with several records of data."

        ...

    async def _execute_typed(
        self,
        statement: str,
        source: DataSource,
        table: Table,
        order: Optional[tuple[str, ...]] = None,
    ) -> None:
        "Executes a SQL statement with several records of data, passing type information."

        await self._execute_all(statement, source)

    async def query_one(self, signature: type[T], statement: str) -> T:
        "Runs a query to produce a result-set of one or more columns, and a single row."

        rows = await self.query_all(signature, statement)
        return rows[0]

    async def query_all(self, signature: type[T], statement: str) -> list[T]:
        "Runs a query to produce a result-set of one or more columns, and multiple rows."

        if LOGGER.isEnabledFor(logging.DEBUG):
            LOGGER.debug(
                "query SQL with into %s:\n%s", python_type_to_str(signature), statement
            )
        try:
            return await self._query_all(signature, statement)
        except QueryException:
            raise
        except Exception as e:
            raise QueryException(statement) from e

    @abc.abstractmethod
    async def _query_all(self, signature: type[T], statement: str) -> list[T]:
        "Runs a query to produce a result-set of one or more columns, and multiple rows."

        ...

    async def current_schema(self) -> Optional[str]:
        func = self.connection.generator.get_current_schema_stmt()
        return await self.query_one(str, f"SELECT {func};")

    async def create_schema(self, namespace: LocalId) -> None:
        LOGGER.debug("create schema: %s", namespace)
        factory = self.connection.generator.factory
        stmt = factory.namespace_class(namespace).create_schema_stmt()
        if stmt:
            await self.execute(stmt)

    async def drop_schema(self, namespace: LocalId) -> None:
        LOGGER.debug("drop schema: %s", namespace)
        factory = self.connection.generator.factory
        stmt = factory.namespace_class(namespace).drop_schema_stmt()
        if stmt:
            await self.execute(stmt)

    def get_table_id(self, ref: ClassRef) -> SupportsQualifiedId:
        return self.connection.generator.get_qualified_id(ref)

    @overload
    def get_table(self, __ref: ClassRef) -> Table: ...

    @overload
    def get_table(self, __table: type[DataclassInstance]) -> Table: ...

    def get_table(
        self, __table_or_ref: Union[ClassRef, type[DataclassInstance]]
    ) -> Table:
        if isinstance(__table_or_ref, ClassRef):
            table_id = self.get_table_id(__table_or_ref)
        elif is_dataclass_type(__table_or_ref):
            table_id = self.get_table_id(ClassRef(__table_or_ref))
        else:
            raise TypeError(f"expected: {ClassRef.__name__} or data-class type")

        return self.connection.generator.state.get_table(table_id)

    async def drop_table_if_exists(self, table: SupportsQualifiedId) -> None:
        LOGGER.debug("drop table if exists: %s", table)
        factory = self.connection.generator.factory
        # column list and primary key are ignored
        stmt = factory.table_class(table, [], primary_key=()).drop_if_exists_stmt()
        if stmt:
            await self.execute(stmt)

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
        records: RecordSource,
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
            LOGGER.debug("insert %d rows into %s", len(records), table.name)
            if not len(records):
                LOGGER.warning("no rows to insert")
                return
        else:
            LOGGER.debug("insert into %s", table.name)

        source = to_data_source(records)
        await self._insert_rows(
            table, source, field_types=field_types, field_names=field_names
        )

        if isinstance(records, Sized):
            LOGGER.info("%d rows have been inserted into %s", len(records), table.name)

    async def _insert_rows(
        self,
        table: Table,
        source: DataSource,
        *,
        field_types: tuple[type, ...],
        field_names: Optional[tuple[str, ...]] = None,
    ) -> None:
        """
        Inserts rows in a database table, ignoring duplicate keys.

        Override in engine-specific derived classes.
        """

        record_generator = await self._generate_records(
            table, source, field_types=field_types, field_names=field_names
        )
        order = tuple(name for name in field_names if name) if field_names else None
        statement = self.connection.generator.get_table_insert_stmt(table, order)
        await self._execute_typed(statement, record_generator, table, order)

    async def upsert_rows(
        self,
        table: Table,
        records: RecordSource,
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
            LOGGER.debug("upsert %d rows into %s", len(records), table.name)
            if not len(records):
                LOGGER.warning("no rows to upsert")
                return
        else:
            LOGGER.debug("upsert into %s", table.name)

        source = to_data_source(records)
        await self._upsert_rows(
            table, source, field_types=field_types, field_names=field_names
        )

        if isinstance(records, Sized):
            LOGGER.info(
                "%d rows have been inserted or updated into %s",
                len(records),
                table.name,
            )

    async def _upsert_rows(
        self,
        table: Table,
        source: DataSource,
        *,
        field_types: tuple[type, ...],
        field_names: Optional[tuple[str, ...]] = None,
    ) -> None:
        """
        Inserts or updates rows in a database table.

        Override in engine-specific derived classes.
        """

        record_generator = await self._generate_records(
            table, source, field_types=field_types, field_names=field_names
        )
        order = tuple(name for name in field_names if name) if field_names else None
        statement = self.connection.generator.get_table_upsert_stmt(table, order)

        await self._execute_typed(statement, record_generator, table, order)

    async def _generate_records(
        self,
        table: Table,
        source: DataSource,
        *,
        field_types: tuple[type, ...],
        field_names: Optional[tuple[str, ...]] = None,
    ) -> DataSource:
        """
        Creates a record generator for a database table.

        Pass a list of field names when the order of fields in the source data does not match the column order
        in the target table.

        :param table: The database table descriptor.
        :param source: The records to insert or update.
        :param field_types: Type of each record field. Use `types.NoneType` to omit a field.
        :param field_names: Label for each record field. Use an empty string for an omitted field.
        """

        generator = self.connection.generator

        if field_names is None:
            field_names = tuple(name for name in table.columns.keys())

        indices: list[int] = []
        transformers: list[RecordTransformer] = []
        for index, field_type, field_name in zip(
            range(len(field_types)), field_types, field_names
        ):
            if field_type is type(None) or not field_name:
                continue

            indices.append(index)
            transformer = self._get_transformer(
                table, generator, index, field_type, field_name
            )
            transformers.append(transformer)

        if all(transformer is None for transformer in transformers):
            if len(indices) == len(field_types):
                return source
            else:
                return SelectorDataSource(source, indices)
        else:
            if len(indices) == len(field_types):
                return TransformerDataSource(source, transformers)
            else:
                return SelectorTransformerDataSource(source, indices, transformers)

    def _get_transformer(
        self,
        table: Table,
        generator: BaseGenerator,
        index: int,
        field_type: type,
        field_name: str,
    ) -> RecordTransformer:
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

        if not table.is_relation(column):
            return ScalarTransformer(transformer)
        relation = generator.state.get_referenced_table(table.name, column.name)
        if not relation.is_lookup_table():
            return ScalarTransformer(transformer)

        LOGGER.debug(
            "found lookup table column %s in table %s", column.name, table.name
        )

        if field_type is str:
            return EnumTransformer(
                self,
                relation,
                generator,
                index,
            )
        elif field_type is list or field_type is set:
            return EnumListTransformer(
                self,
                relation,
                generator,
                index,
            )
        else:
            return ScalarTransformer(transformer)

    async def delete_rows(
        self, table: Table, key_type: type, key_values: Iterable[Any]
    ) -> None:
        """
        Deletes rows from a database table.

        :param table: The table to remove records from.
        :param key_type: The data type of the key values such that the following is true:
            `all(isinstance(v, key_type) for v in key_values)`
        :param key_values: The key values to look up in the table.
        """

        if isinstance(key_values, Sized):
            LOGGER.debug("delete %d rows from %s", len(key_values), table.name)
            if not len(key_values):
                LOGGER.warning("no rows to delete")
                return
        else:
            LOGGER.debug("delete from %s", table.name)

        await self._delete_rows(table, key_type, key_values)

        if isinstance(key_values, Sized):
            LOGGER.info(
                "%d rows have been deleted from %s", len(key_values), table.name
            )

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
            source = IterableDataSource((transformer(key),) for key in key_values)
        else:
            source = IterableDataSource((key,) for key in key_values)

        statement = generator.get_table_delete_stmt(table)
        order = (table.get_primary_column().name.local_id,)
        await self._execute_typed(statement, source, table, order)


def _module_or_list(
    module: Optional[types.ModuleType], modules: Optional[list[types.ModuleType]]
) -> list[types.ModuleType]:
    if module is None and modules is None:
        raise TypeError("required: one of parameters `module` and `modules`")
    if module is not None and modules is not None:
        raise TypeError("disallowed: both parameters `module` and `modules`")

    if modules is not None:
        if not isinstance(modules, list):
            raise TypeError("expected: list of modules for parameter `modules`")
        entity_modules = modules
    elif module is not None:
        entity_modules = [module]
    else:
        # should never be triggered; either `module` or `modules` must be defined at this point
        raise NotImplementedError("match condition not exhaustive")

    return entity_modules


class DiscoveryError(RuntimeError):
    pass


class Explorer(abc.ABC):
    conn: BaseContext

    def __init__(self, conn: BaseContext) -> None:
        self.conn = conn

    def get_qualified_id(
        self, namespace: Optional[str], id: str
    ) -> SupportsQualifiedId:
        return QualifiedId(namespace, id)

    @abc.abstractmethod
    async def get_table_names(self) -> list[QualifiedId]: ...

    @abc.abstractmethod
    async def has_table(self, table_id: SupportsQualifiedId) -> bool: ...

    @abc.abstractmethod
    async def has_column(
        self, table_id: SupportsQualifiedId, column_id: LocalId
    ) -> bool: ...

    @abc.abstractmethod
    async def get_table(self, table_id: SupportsQualifiedId) -> Table: ...

    @abc.abstractmethod
    async def get_namespace(self, namespace_id: LocalId) -> Namespace:
        "Constructs a database object model of a namespace (database schema)."
        ...

    @abc.abstractmethod
    async def get_namespace_current(self) -> Namespace:
        "Constructs a database object model of the current namespace (database schema)."
        ...

    @overload
    async def discover(self, *, module: types.ModuleType) -> None: ...

    @overload
    async def discover(self, *, modules: list[types.ModuleType]) -> None: ...

    async def discover(
        self,
        *,
        module: Optional[types.ModuleType] = None,
        modules: Optional[list[types.ModuleType]] = None,
    ) -> None:
        """
        Assumes a database model based on the state of the database.

        Discovers database objects (e.g. tables, structs, user-defined types, constraints) and builds a model
        representing those objects. The database model is specific to the selected dialect.

        The list of Python modules passed as a parameter is used in mapping modules to database schemas. These modules
        don't need to contain Python class definitions.

        :param module: The Python module identifying the database schema to discover.
        :param modules: The list of Python modules identifying database schemas to discover.
        """

        entity_modules = _module_or_list(module, modules)

        generator = self.conn.connection.generator
        generator.reset()

        for entity_module in entity_modules:
            ns_name = generator.converter.options.namespaces.get(entity_module.__name__)
            if ns_name is not None:
                ns = await self.get_namespace(LocalId(ns_name))
            else:
                ns = await self.get_namespace_current()
            generator.state.merge(Catalog([ns]))

        LOGGER.debug("found %d namespaces", len(generator.state.namespaces))
        for ns in generator.state.namespaces.values():
            LOGGER.debug("found %d enum(s) in namespace %s", len(ns.enums), ns.name)
            LOGGER.debug("found %d struct(s) in namespace %s", len(ns.structs), ns.name)
            LOGGER.debug("found %d table(s) in namespace %s", len(ns.tables), ns.name)
        LOGGER.debug("discovered state:\n%s", generator.state)

    @overload
    async def synchronize(self, *, module: types.ModuleType) -> None: ...

    @overload
    async def synchronize(self, *, modules: list[types.ModuleType]) -> None: ...

    async def synchronize(
        self,
        *,
        module: Optional[types.ModuleType] = None,
        modules: Optional[list[types.ModuleType]] = None,
    ) -> None:
        """
        Synchronizes a current source state with a desired target state.

        First, constructs a database source model from the Python modules and the classes defined within.

        Next, discovers database objects (e.g. tables, structs, user-defined types, constraints) and builds a target
        model representing those objects. The database model is specific to the selected dialect.

        Finally, compares the source and the target models, and mutates the database state from the source state to
        the desired target state. Mutation involves executing SQL statements.

        :param module: The Python module identifying the database schema to discover.
        :param modules: The list of Python modules identifying database schemas to discover.
        """

        entity_modules = _module_or_list(module, modules)

        generator = self.conn.connection.generator

        # determine target database schema
        generator.reset()
        generator.create(tables=get_entity_types(entity_modules))
        target_state = generator.state
        LOGGER.debug("desired state:\n%s", generator.state)

        # acquire current database schema
        await self.discover(modules=entity_modules)

        # mutate current state into desired state
        stmt = generator.get_mutate_stmt(target_state)
        if stmt is not None:
            LOGGER.info("synchronize schema with SQL:\n%s", stmt)
            await self.conn.execute(stmt)
            generator.state = target_state


class BaseEngine(abc.ABC):
    "Represents a specific database server type."

    @property
    @abc.abstractmethod
    def name(self) -> str: ...

    @abc.abstractmethod
    def get_generator_type(self) -> type[BaseGenerator]: ...

    @abc.abstractmethod
    def get_connection_type(self) -> type[BaseConnection]: ...

    @abc.abstractmethod
    def get_explorer_type(self) -> type[Explorer]: ...

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
