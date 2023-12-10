import logging
import re
import typing
from typing import Any, Iterable, TypeVar

import oracledb
from strong_typing.inspection import DataclassInstance

from pysqlsync.base import BaseConnection, BaseContext
from pysqlsync.util.dispatch import thread_dispatch
from pysqlsync.util.typing import override

D = TypeVar("D", bound=DataclassInstance)
T = TypeVar("T")

LOGGER = logging.getLogger("pysqlsync.oracle")


class OracleConnection(BaseConnection):
    native: oracledb.Connection

    @override
    @thread_dispatch
    def open(self) -> BaseContext:
        LOGGER.info(f"connecting to {self.params}")

        host = self.params.host or "localhost"
        port = self.params.port or 1521
        database = self.params.database or "FREEPDB1"

        conn = oracledb.connect(
            user=self.params.username,
            password=self.params.password,
            dsn=f"{host}:{port}/{database}",
        )

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
                    cur.execute(s)

    @override
    @thread_dispatch
    def _execute_all(self, statement: str, args: Iterable[tuple[Any, ...]]) -> None:
        statement = statement.rstrip("\r\n\t\v ;")
        with self.native_connection.cursor() as cur:
            cur.executemany(statement, list(args))

    @override
    @thread_dispatch
    def _query_all(self, signature: type[T], statement: str) -> list[T]:
        with self.native_connection.cursor() as cur:
            cur.execute(statement)
            records = cur.fetchall()
            return self._resultset_unwrap_tuple(signature, records)
