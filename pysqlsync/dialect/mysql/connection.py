import logging
import typing
from typing import AsyncIterable, Iterable, Optional, TypeVar

import aiomysql
from strong_typing.inspection import DataclassInstance, is_dataclass_type

from pysqlsync.base import BaseConnection, BaseContext, RecordIterable
from pysqlsync.model.data_types import escape_like
from pysqlsync.model.id_types import LocalId
from pysqlsync.resultset import resultset_unwrap_dict, resultset_unwrap_tuple
from pysqlsync.util.typing import override
from pysqlsync.util.unsync import unsync

D = TypeVar("D", bound=DataclassInstance)
T = TypeVar("T")

LOGGER = logging.getLogger("pysqlsync.mysql")


def quoted_id(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


class MySQLConnection(BaseConnection):
    native: aiomysql.Connection

    @override
    async def open(self) -> BaseContext:
        LOGGER.info("connecting to %s (with aiomysql)", self.params)
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
            autocommit=True,
        )
        return MySQLContext(self)

    @override
    async def close(self) -> None:
        self.native.close()


class MySQLContext(BaseContext):
    def __init__(self, connection: MySQLConnection) -> None:
        super().__init__(connection)

    @property
    def native_connection(self) -> aiomysql.Connection:
        return typing.cast(MySQLConnection, self.connection).native

    @override
    async def _execute(self, statement: str) -> None:
        async with self.native_connection.cursor() as cur:
            await cur.execute(statement)

    @override
    async def _execute_all(self, statement: str, records: RecordIterable) -> None:
        if isinstance(records, Iterable):
            async with self.native_connection.cursor() as cur:
                await cur.executemany(statement, records)
        elif isinstance(records, AsyncIterable):
            async with self.native_connection.cursor() as cur:
                await cur.executemany(statement, await unsync(records))
        else:
            raise TypeError("expected: `Iterable` or `AsyncIterable` of records")

    @override
    async def _query_all(self, signature: type[T], statement: str) -> list[T]:
        if is_dataclass_type(signature):
            cur = await self.native_connection.cursor(aiomysql.cursors.DictCursor)
            await cur.execute(statement)
            records = await cur.fetchall()
            return resultset_unwrap_dict(signature, records)  # type: ignore
        else:
            cur = await self.native_connection.cursor()
            await cur.execute(statement)
            records = await cur.fetchall()
            return resultset_unwrap_tuple(signature, records)

    @override
    async def current_schema(self) -> Optional[str]:
        return None

    @override
    async def drop_schema(self, namespace: LocalId) -> None:
        LOGGER.debug("drop schema: %s", namespace)

        tables = await self.query_all(
            str,
            "SELECT table_name\n"
            "FROM information_schema.tables\n"
            f"WHERE table_schema = DATABASE() AND table_name LIKE '{escape_like(namespace.id, '~')}~_~_%' ESCAPE '~';",
        )
        if tables:
            table_list = ", ".join(quoted_id(table) for table in tables)
            await self.execute(f"DROP TABLE IF EXISTS {table_list};")
