import types
import typing
from typing import Any, Iterable, Optional, TypeVar

import aiomysql
from strong_typing.inspection import DataclassInstance

from pysqlsync.base import BaseConnection, BaseContext

D = TypeVar("D", bound=DataclassInstance)
T = TypeVar("T")


class MySQLConnection(BaseConnection):
    native: aiomysql.Connection

    async def __aenter__(self) -> BaseContext:
        sql_mode = ",".join(["ANSI_QUOTES", "STRICT_ALL_TABLES"])
        self.native = await aiomysql.connect(
            host=self.params.host,
            port=self.params.port,
            user=self.params.username,
            password=self.params.password,
            db=self.params.database,
            sql_mode=f"'{sql_mode}'",
            init_command='SET @@session.time_zone = "+00:00";',
        )
        return MySQLContext(self)

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> None:
        self.native.close()


class MySQLContext(BaseContext):
    def __init__(self, connection: MySQLConnection) -> None:
        super().__init__(connection)

    @property
    def native_connection(self) -> aiomysql.Connection:
        return typing.cast(MySQLConnection, self.connection).native

    async def execute(self, statement: str) -> None:
        async with self.native_connection.cursor() as cur:
            await cur.execute(statement)

    async def execute_all(
        self, statement: str, args: Iterable[tuple[Any, ...]]
    ) -> None:
        async with self.native_connection.cursor() as cur:
            await cur.executemany(statement, args)

    async def query_all(self, signature: type[T], statement: str) -> list[T]:
        cur = await self.native_connection.cursor()
        await cur.execute(statement)
        records = await cur.fetchall()
        return [tuple(record) for record in records]  # type: ignore
