import typing
from typing import Iterable, Optional, TypeVar

import aiotrino
from strong_typing.inspection import is_dataclass_type

from pysqlsync.base import BaseConnection, BaseContext, DataSource
from pysqlsync.formation.object_types import Table
from pysqlsync.resultset import resultset_unwrap_tuple
from pysqlsync.util.typing import override

T = TypeVar("T")


class TrinoConnection(BaseConnection):
    native: aiotrino.dbapi.Connection

    @override
    async def open(self) -> BaseContext:
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

    @override
    async def close(self) -> None:
        await self.native.close()


class TrinoContext(BaseContext):
    def __init__(self, connection: TrinoConnection) -> None:
        super().__init__(connection)

    @property
    def native_connection(self) -> aiotrino.dbapi.Connection:
        return typing.cast(TrinoConnection, self.connection).native

    @override
    async def _execute(self, statement: str) -> None:
        cur = await self.native_connection.cursor()
        await cur.execute(statement)

    @override
    async def _execute_all(self, statement: str, source: DataSource) -> None:
        raise NotImplementedError("operation not supported for Trino")

    @override
    async def _query_all(self, signature: type[T], statement: str) -> list[T]:
        if is_dataclass_type(signature):
            raise NotImplementedError()

        cur = await self.native_connection.cursor()
        await cur.execute(statement)
        records = await cur.fetchall()
        return resultset_unwrap_tuple(signature, records)

    @override
    async def insert_data(self, table: type[T], data: Iterable[T]) -> None:
        raise NotImplementedError("operation not supported for Trino")

    @override
    async def _insert_rows(
        self,
        table: Table,
        records: DataSource,
        *,
        field_types: tuple[type, ...],
        field_names: Optional[tuple[str, ...]] = None,
    ) -> None:
        raise NotImplementedError("operation not supported for Trino")

    @override
    async def _upsert_rows(
        self,
        table: Table,
        records: DataSource,
        *,
        field_types: tuple[type, ...],
        field_names: Optional[tuple[str, ...]] = None,
    ) -> None:
        raise NotImplementedError("operation not supported for Trino")
