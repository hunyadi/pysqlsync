import logging
import types
import typing
from typing import Any, Iterable, Optional, TypeVar

import pyodbc
from strong_typing.inspection import is_dataclass_type

from pysqlsync.base import BaseConnection, BaseContext
from pysqlsync.model.data_types import quote
from pysqlsync.model.id_types import LocalId, QualifiedId

T = TypeVar("T")

LOGGER = logging.getLogger("pysqlsync.mssql")


class MSSQLConnection(BaseConnection):
    """
    Represents a connection to a Microsoft SQL Server.

    If Microsoft SQL Server is run in a Docker container in a Linux/MacOS environment, use:
    ```
    docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=<YourStrong@Passw0rd>" -p 1433:1433 --name sql1 --hostname sql1 -d mcr.microsoft.com/mssql/server:2022-latest
    ```
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

    async def _execute(self, statement: str) -> None:
        with self.native_connection.cursor() as cur:
            cur.execute(statement)

    async def _execute_all(
        self, statement: str, args: Iterable[tuple[Any, ...]]
    ) -> None:
        with self.native_connection.cursor() as cur:
            cur.fast_executemany = True
            cur.executemany(statement, args)

    async def _query_all(self, signature: type[T], statement: str) -> list[T]:
        with self.native_connection.cursor() as cur:
            records = cur.execute(statement).fetchall()

            if is_dataclass_type(signature):
                return self._resultset_unwrap_object(signature, records)  # type: ignore
            else:
                return self._resultset_unwrap_tuple(signature, records)

    async def current_schema(self) -> Optional[str]:
        return await self.query_one(str, "SELECT SCHEMA_NAME();")

    async def create_schema(self, namespace: LocalId) -> None:
        LOGGER.debug(f"create schema: {namespace}")

        # Microsoft SQL Server requires a separate batch for creating a schema
        await self.execute(
            f"IF NOT EXISTS ( SELECT * FROM sys.schemas WHERE name = N{quote(namespace.id)} ) EXEC('CREATE SCHEMA {namespace}');"
        )

    async def drop_schema(self, namespace: LocalId) -> None:
        LOGGER.debug(f"drop schema: {namespace}")

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
