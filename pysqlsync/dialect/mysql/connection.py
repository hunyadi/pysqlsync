import types
import typing
from typing import Any, Iterable, Optional, TypeVar

import aiomysql
from pysqlsync.base import BaseConnection, BaseContext
from pysqlsync.model.data_types import escape_like
from pysqlsync.model.id_types import LocalId
from strong_typing.inspection import DataclassInstance, is_dataclass_type

D = TypeVar("D", bound=DataclassInstance)
T = TypeVar("T")


class MySQLConnection(BaseConnection):
    native: aiomysql.Connection

    async def __aenter__(self) -> BaseContext:
        sql_mode = ",".join(
            [
                "ANSI_QUOTES",
                "NO_AUTO_VALUE_ON_ZERO",
                "STRICT_ALL_TABLES",
            ]
        )
        self.native = await aiomysql.connect(
            host=self.params.host or "localhost",
            port=self.params.port or 3306,
            user=self.params.username,
            password=self.params.password or "",
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
        if is_dataclass_type(signature):
            cur = await self.native_connection.cursor(aiomysql.cursors.DictCursor)
            await cur.execute(statement)
            records = await cur.fetchall()
            return self._resultset_unwrap_dict(signature, records)  # type: ignore
        else:
            cur = await self.native_connection.cursor()
            await cur.execute(statement)
            records = await cur.fetchall()
            return self._resultset_unwrap_tuple(signature, records)

    async def drop_schema(self, namespace: LocalId) -> None:
        statement = await self.query_one(
            str,
            "SELECT CONCAT('DROP TABLE IF EXISTS ', GROUP_CONCAT(table_name), ';') AS statement\n"
            "FROM information_schema.tables\n"
            f"WHERE table_schema = DATABASE() AND table_name LIKE '{escape_like(namespace.id, '~')}~_~_%' ESCAPE '~';",
        )
        if statement:
            await self.execute(statement)
