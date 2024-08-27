import dataclasses
import logging
import re
import typing
from io import BytesIO
from typing import Iterable, Optional, TypeVar

import redshift_connector
from strong_typing.inspection import DataclassInstance, is_dataclass_type

from pysqlsync.base import (
    BaseConnection,
    BaseContext,
    ClassRef,
    DataSource,
    QueryException,
    RecordType,
)
from pysqlsync.formation.object_types import Table
from pysqlsync.model.id_types import LocalId, SupportsQualifiedId
from pysqlsync.model.properties import is_identity_type
from pysqlsync.resultset import resultset_unwrap_object, resultset_unwrap_tuple
from pysqlsync.util.dispatch import thread_dispatch
from pysqlsync.util.typing import override

D = TypeVar("D", bound=DataclassInstance)
T = TypeVar("T")

LOGGER = logging.getLogger("pysqlsync.redshift")

redshift_connector.paramstyle = "numeric"


class RedshiftConnection(BaseConnection):
    native: redshift_connector.Connection

    @override
    @thread_dispatch
    def open(self) -> BaseContext:
        LOGGER.info("connecting to %s", self.params)
        conn = redshift_connector.connect(
            iam=True,
            host=self.params.host,
            port=self.params.port,
            database=self.params.database,
            db_user=self.params.username,
        )
        conn.autocommit = True

        self.native = conn
        return RedshiftContext(self)

    @override
    @thread_dispatch
    def close(self) -> None:
        self.native.close()


class RedshiftContext(BaseContext):
    def __init__(self, connection: RedshiftConnection) -> None:
        super().__init__(connection)

    @property
    def native_connection(self) -> redshift_connector.Connection:
        return typing.cast(RedshiftConnection, self.connection).native

    @override
    @thread_dispatch
    def _execute(self, statement: str) -> None:
        with self.native_connection.cursor() as cursor:
            # The library `redshift_connector` executes all statements as prepared statements, which locks out
            # the possibility to execute multiple statements in one go. We artificially split SQL scripts into
            # multiple statements to work around this limitation in the connector.
            statement = statement.rstrip("\r\n\t\v ;")
            for s in re.split(r";$", statement, flags=re.MULTILINE):
                try:
                    cursor.execute(s)
                except Exception as e:
                    raise QueryException(s) from e

    @override
    async def _execute_all(self, statement: str, source: DataSource) -> None:
        async for batch in source.batches():
            await self._internal_execute_all(statement, batch)

    @thread_dispatch
    def _internal_execute_all(
        self, statement: str, records: Iterable[RecordType]
    ) -> None:
        with self.native_connection.cursor() as cursor:
            cursor.executemany(statement, records)

    @override
    @thread_dispatch
    def _query_all(self, signature: type[T], statement: str) -> list[T]:
        with self.native_connection.cursor() as cursor:
            records = cursor.execute(statement).fetchall()

            if is_dataclass_type(signature):
                return resultset_unwrap_object(signature, records)  # type: ignore
            else:
                return resultset_unwrap_tuple(signature, records)

    @override
    @thread_dispatch
    def insert_data(self, table: type[D], data: Iterable[D]) -> None:
        if not is_dataclass_type(table):
            raise TypeError(f"expected dataclass type, got: {table}")
        generator = self.connection.generator
        table_name = generator.get_qualified_id(ClassRef(table))
        columns = [
            LocalId(field.name)
            for field in dataclasses.fields(table)
            if not is_identity_type(field.type)
        ]
        records = generator.get_dataclasses_as_records(table, data, skip_identity=True)
        self._insert_copy_stream(table_name, columns, records)

    @override
    async def _insert_rows(
        self,
        table: Table,
        source: DataSource,
        *,
        field_types: tuple[type, ...],
        field_names: Optional[tuple[str, ...]] = None,
    ) -> None:
        order = tuple(name for name in field_names if name) if field_names else None
        columns = [col.name for col in table.get_columns(order)]
        record_generator = await self._generate_records(
            table, source, field_types=field_types, field_names=field_names
        )
        async for batch in record_generator.batches():
            await self._insert_copy_stream_async(table.name, columns, batch)

    @thread_dispatch
    def _insert_copy_stream_async(
        self,
        table_name: SupportsQualifiedId,
        columns: list[LocalId],
        records: Iterable[RecordType],
    ) -> None:
        self._insert_copy_stream(table_name, columns, records)

    def _insert_copy_stream(
        self,
        table_name: SupportsQualifiedId,
        columns: list[LocalId],
        records: Iterable[RecordType],
    ) -> None:
        column_list = ", ".join(str(column) for column in columns)
        copy_query = f"COPY {table_name} ({column_list}) FROM STDIN"

        with BytesIO() as f:
            for record in records:
                f.write(b"|".join(str(value).encode("utf-8") for value in record))
                f.write(b"\n")
            stream_data = f.getvalue()

        cursor = self.native_connection.cursor()
        try:
            cursor.execute(copy_query, stream=BytesIO(stream_data))
        finally:
            cursor.close()
