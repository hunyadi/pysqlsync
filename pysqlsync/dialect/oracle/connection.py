import logging
import re
import typing
from typing import Iterable, Optional, TypeVar

import oracledb
from strong_typing.inspection import DataclassInstance, is_dataclass_type

from pysqlsync.base import (
    BaseConnection,
    BaseContext,
    DataSource,
    QueryException,
    RecordType,
)
from pysqlsync.formation.object_types import Table
from pysqlsync.model.data_types import escape_like
from pysqlsync.model.id_types import LocalId
from pysqlsync.resultset import resultset_unwrap_tuple
from pysqlsync.util.dispatch import thread_dispatch
from pysqlsync.util.typing import override

from .data_types import sql_to_oracle_type

D = TypeVar("D", bound=DataclassInstance)
T = TypeVar("T")

LOGGER = logging.getLogger("pysqlsync.oracle")


class OracleConnection(BaseConnection):
    native: oracledb.Connection

    @override
    @thread_dispatch
    def open(self) -> BaseContext:
        LOGGER.info("connecting to %s", self.params)

        host = self.params.host or "localhost"
        port = self.params.port or 1521
        database = self.params.database or "FREEPDB1"

        conn = oracledb.connect(
            user=self.params.username,
            password=self.params.password,
            dsn=f"{host}:{port}/{database}",
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")

        self.native = conn
        return OracleContext(self)

    @override
    @thread_dispatch
    def close(self) -> None:
        self.native.close()


class OracleContext(BaseContext):
    def __init__(self, connection: OracleConnection) -> None:
        super().__init__(connection)

    @property
    def native_connection(self) -> oracledb.Connection:
        return typing.cast(OracleConnection, self.connection).native

    @override
    @thread_dispatch
    def _execute(self, statement: str) -> None:
        with self.native_connection.cursor() as cur:
            if statement.startswith("BEGIN"):
                cur.execute(statement)
            else:
                statement = statement.rstrip("\r\n\t\v ;")
                for s in re.split(r";$", statement, flags=re.MULTILINE):
                    try:
                        cur.execute(s)
                    except Exception as e:
                        raise QueryException(s) from e

    @override
    async def _execute_all(self, statement: str, source: DataSource) -> None:
        async for batch in source.batches():
            await self._internal_execute_all(statement, batch)

    @override
    async def _execute_typed(
        self,
        statement: str,
        source: DataSource,
        table: Table,
        order: Optional[tuple[str, ...]] = None,
    ) -> None:
        async for batch in source.batches():
            await self._internal_execute_typed(statement, batch, table, order)

    @thread_dispatch
    def _internal_execute_all(
        self, statement: str, records: Iterable[RecordType]
    ) -> None:
        statement = statement.rstrip("\r\n\t\v ;")
        with self.native_connection.cursor() as cur:
            cur.executemany(statement, list(records))

    @thread_dispatch
    def _internal_execute_typed(
        self,
        statement: str,
        records: Iterable[RecordType],
        table: Table,
        order: Optional[tuple[str, ...]] = None,
    ) -> None:
        statement = statement.rstrip("\r\n\t\v ;")
        with self.native_connection.cursor() as cur:
            cur.setinputsizes(
                *tuple(
                    sql_to_oracle_type(cur, column.data_type)
                    for column in table.get_columns(order)
                )
            )
            cur.executemany(statement, list(records))

    @override
    @thread_dispatch
    def _query_all(self, signature: type[T], statement: str) -> list[T]:
        with self.native_connection.cursor() as cur:
            cur.execute(statement)
            if is_dataclass_type(signature):
                cur.rowfactory = lambda *args: signature(*args)

            records = cur.fetchall()

            if is_dataclass_type(signature):
                return records
            else:
                return resultset_unwrap_tuple(signature, records)

    @override
    async def current_schema(self) -> Optional[str]:
        return None

    @override
    async def drop_schema(self, namespace: LocalId) -> None:
        LOGGER.debug("drop schema: %s", namespace)
        condition = (
            f"table_name LIKE '{escape_like(namespace.id, '~')}~_~_%' ESCAPE '~'"
        )
        tables = await self.query_all(
            str,
            f"SELECT table_name FROM dba_tables WHERE {condition}",
        )
        statement = "\n".join(
            f"DROP TABLE {LocalId(table)} CASCADE CONSTRAINTS PURGE;"
            for table in tables
        )
        if statement:
            await self.execute(statement)
