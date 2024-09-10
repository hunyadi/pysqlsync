import logging
import typing
from typing import Iterable, Optional, TypeVar

import pyodbc
from strong_typing.inspection import is_dataclass_type

from pysqlsync.base import BaseConnection, BaseContext, DataSource, RecordType
from pysqlsync.formation.object_types import Table
from pysqlsync.model.data_types import quote
from pysqlsync.model.id_types import LocalId, QualifiedId
from pysqlsync.resultset import resultset_unwrap_object, resultset_unwrap_tuple
from pysqlsync.util.dispatch import thread_dispatch
from pysqlsync.util.typing import override

from .data_types import sql_to_odbc_type

T = TypeVar("T")

LOGGER = logging.getLogger("pysqlsync.mssql")


class MSSQLConnection(BaseConnection):
    """
    Represents a connection to a Microsoft SQL Server.
    """

    native: pyodbc.Connection

    @override
    @thread_dispatch
    def open(self) -> BaseContext:
        LOGGER.info("connecting to %s", self.params)
        params = {
            "DRIVER": "{ODBC Driver 18 for SQL Server}",
            "SERVER": (
                f"{self.params.host},{self.params.port}"
                if self.params.port is not None
                else self.params.host
            ),
            "UID": self.params.username,
            "PWD": self.params.password,
            "TrustServerCertificate": "yes",
        }
        if self.params.database is not None:
            params["DATABASE"] = self.params.database
        conn_string = ";".join(
            f"{key}={value}" for key, value in params.items() if value is not None
        )
        conn = pyodbc.connect(conn_string)
        with conn.cursor() as cur:
            rows = cur.execute("SELECT @@VERSION").fetchall()
            for row in rows:
                LOGGER.info(row)

        self.native = conn
        return MSSQLContext(self)

    @override
    @thread_dispatch
    def close(self) -> None:
        self.native.close()


class MSSQLContext(BaseContext):
    def __init__(self, connection: MSSQLConnection) -> None:
        super().__init__(connection)

    @property
    def native_connection(self) -> pyodbc.Connection:
        return typing.cast(MSSQLConnection, self.connection).native

    @override
    @thread_dispatch
    def _execute(self, statement: str) -> None:
        with self.native_connection.cursor() as cur:
            cur.execute(statement)

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
        with self.native_connection.cursor() as cur:
            cur.fast_executemany = True
            cur.executemany(statement, records)

    @thread_dispatch
    def _internal_execute_typed(
        self,
        statement: str,
        records: Iterable[RecordType],
        table: Table,
        order: Optional[tuple[str, ...]] = None,
    ) -> None:
        with self.native_connection.cursor() as cur:
            cur.fast_executemany = True
            cur.setinputsizes(
                [
                    sql_to_odbc_type(column.data_type)
                    for column in table.get_columns(order)
                ]
            )
            cur.executemany(statement, records)

    @override
    @thread_dispatch
    def _query_all(self, signature: type[T], statement: str) -> list[T]:
        with self.native_connection.cursor() as cur:
            records = cur.execute(statement).fetchall()

            if is_dataclass_type(signature):
                return resultset_unwrap_object(signature, records)  # type: ignore
            else:
                return resultset_unwrap_tuple(signature, records)

    @override
    async def drop_schema(self, namespace: LocalId) -> None:
        LOGGER.debug("drop schema: %s", namespace)

        constraints = await self.query_all(
            tuple[str, str],
            "SELECT table_name, constraint_name\n"
            "FROM information_schema.table_constraints\n"
            f"WHERE constraint_type = 'FOREIGN KEY' AND table_schema = {quote(namespace.id)};",
        )
        if constraints:
            stmts: list[str] = []
            for constraint in constraints:
                table_name, constraint_name = constraint
                stmts.append(
                    f"ALTER TABLE {QualifiedId(namespace.id, table_name)} DROP CONSTRAINT {LocalId(constraint_name)};"
                )
            await self.execute("\n".join(stmts))

        tables = await self.query_all(
            str,
            "SELECT table_name\n"
            "FROM information_schema.tables\n"
            f"WHERE table_schema = {quote(namespace.id)};",
        )
        if tables:
            table_list = ", ".join(
                str(QualifiedId(namespace.local_id, table)) for table in tables
            )
            await self.execute(f"DROP TABLE IF EXISTS {table_list};")

        await self.execute(f"DROP SCHEMA IF EXISTS {namespace};")
