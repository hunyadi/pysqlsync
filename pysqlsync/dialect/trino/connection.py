import types
import typing
from typing import Any, Iterable, Optional, TypeVar

import aiotrino
from strong_typing.inspection import is_dataclass_type

from pysqlsync.base import BaseConnection, BaseContext

T = TypeVar("T")


class TrinoConnection(BaseConnection):
    native: aiotrino.dbapi.Connection

    async def __aenter__(self) -> BaseContext:
        self.native = aiotrino.dbapi.connect(
            host=self.params.host,
            port=self.params.port,
            user=self.params.username,
            auth=aiotrino.auth.BasicAuthentication(
                self.params.username, self.params.password
            ),
            http_scheme="https",
            catalog=self.params.database,
        )
        return TrinoContext(self)

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> None:
        await self.native.close()


class TrinoContext(BaseContext):
    def __init__(self, connection: TrinoConnection) -> None:
        super().__init__(connection)

    @property
    def native_connection(self) -> aiotrino.dbapi.Connection:
        return typing.cast(TrinoConnection, self.connection).native

    async def execute(self, statement: str) -> None:
        cur = await self.native_connection.cursor()
        await cur.execute(statement)

    async def execute_all(
        self, statement: str, args: Iterable[tuple[Any, ...]]
    ) -> None:
        raise NotImplementedError()

    async def query_all(self, signature: type[T], statement: str) -> list[T]:
        if is_dataclass_type(signature):
            raise NotImplementedError()

        cur = await self.native_connection.cursor()
        await cur.execute(statement)
        records = await cur.fetchall()
        return self._resultset_unwrap_tuple(signature, records)

    async def insert_data(self, table: type[T], data: Iterable[T]) -> None:
        raise NotImplementedError()
