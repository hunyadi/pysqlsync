import logging
import types
import typing
from typing import Any, Iterable, Optional, TypeVar

import pyodbc

from pysqlsync.base import BaseConnection, BaseContext

T = TypeVar("T")

LOGGER = logging.getLogger("pysqlsync.mssql")


class MSSQLConnection(BaseConnection):
    """
    Represents a connection to a Microsoft SQL Server.
    """

    native: pyodbc.Connection

    async def __aenter__(self) -> BaseContext:
        LOGGER.info(f"connecting to {self.params}")
        params = {
            "DRIVER": "{ODBC Driver 18 for SQL Server}",
            "SERVER": f"{self.params.host},{self.params.port}"
            if self.params.port is not None
            else self.params.host,
            "UID": self.params.username,
            "PWD": self.params.password,
            "TrustServerCertificate": "yes",
        }
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

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> None:
        self.native.close()


class MSSQLContext(BaseContext):
    def __init__(self, connection: MSSQLConnection) -> None:
        super().__init__(connection)

    @property
    def native_connection(self) -> pyodbc.Connection:
        return typing.cast(MSSQLConnection, self.connection).native

    async def execute(self, statement: str) -> None:
        with self.native_connection.cursor() as cur:
            cur.execute(statement)

    async def execute_all(
        self, statement: str, args: Iterable[tuple[Any, ...]]
    ) -> None:
        with self.native_connection.cursor() as cur:
            # cur.fast_executemany = True
            cur.executemany(statement, args)

    async def query_all(self, signature: type[T], statement: str) -> list[T]:
        with self.native_connection.cursor() as cur:
            records = cur.execute(statement).fetchall()
            return self._resultset_unwrap_tuple(signature, records)
