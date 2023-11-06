import logging
import typing
from typing import Any, Iterable, TypeVar

import MySQLdb
from strong_typing.inspection import DataclassInstance, is_dataclass_type

from pysqlsync.base import BaseConnection, BaseContext
from pysqlsync.util.dispatch import thread_dispatch
from pysqlsync.util.typing import override

from ..connection import MySQLContextBase

D = TypeVar("D", bound=DataclassInstance)
T = TypeVar("T")

LOGGER = logging.getLogger("pysqlsync.mysql.mysqlclient")


class MySQLConnection(BaseConnection):
    native: MySQLdb.Connection

    @override
    async def open(self) -> BaseContext:
        LOGGER.info(f"connecting to {self.params} (with mysqlclient)")
        self.native = MySQLdb.connect(
            host=self.params.host or "localhost",
            port=self.params.port or 3306,
            user=self.params.username,
            password=self.params.password or "",
            db=self.params.database,
            sql_mode=",".join(
                [
                    "ANSI_QUOTES",
                    "NO_AUTO_VALUE_ON_ZERO",
                    "STRICT_ALL_TABLES",
                ]
            ),
            init_command='SET @@session.time_zone = "+00:00";',
        )
        return MySQLContext(self)

    @override
    async def close(self) -> None:
        self.native.close()


class MySQLContext(MySQLContextBase):
    def __init__(self, connection: MySQLConnection) -> None:
        super().__init__(connection)

    @property
    def native_connection(self) -> MySQLdb.Connection:
        return typing.cast(MySQLConnection, self.connection).native

    @thread_dispatch
    @override
    def _execute(self, statement: str) -> None:
        with self.native_connection.cursor() as cur:
            cur.execute(statement)

    @thread_dispatch
    @override
    def _execute_all(self, statement: str, args: Iterable[tuple[Any, ...]]) -> None:
        with self.native_connection.cursor() as cur:
            cur.executemany(statement, args)

    @thread_dispatch
    @override
    def _query_all(self, signature: type[T], statement: str) -> list[T]:
        if is_dataclass_type(signature):
            cur = self.native_connection.cursor(MySQLdb.cursors.DictCursor)
            cur.execute(statement)
            records = cur.fetchall()
            return self._resultset_unwrap_dict(signature, records)  # type: ignore
        else:
            cur = self.native_connection.cursor()
            cur.execute(statement)
            records = cur.fetchall()
            return self._resultset_unwrap_tuple(signature, records)
