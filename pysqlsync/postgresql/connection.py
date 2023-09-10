import dataclasses
import types
import typing
from typing import Any, Iterable, Optional, TypeVar

import asyncpg
from strong_typing.inspection import is_dataclass_type

from ..base import BaseConnection, BaseContext

T = TypeVar("T")


class Connection(BaseConnection):
    native: asyncpg.Connection

    async def __aenter__(self) -> BaseContext:
        self.native = await asyncpg.connect(
            host=self.params.host,
            port=self.params.port,
            user=self.params.username,
            password=self.params.password,
            database=self.params.database,
        )
        return Context(self)

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> None:
        await self.native.close()


class Context(BaseContext):
    def __init__(self, connection: Connection) -> None:
        super().__init__(connection)

    @property
    def native_connection(self) -> asyncpg.Connection:
        return typing.cast(Connection, self.connection).native

    async def execute(self, statement: str) -> None:
        await self.native_connection.execute(statement)

    async def execute_all(
        self, statement: str, args: Iterable[tuple[Any, ...]]
    ) -> None:
        await self.native_connection.executemany(statement, args)

    async def query_all(self, signature: type[T], statement: str) -> list[T]:
        records: list[asyncpg.Record] = await self.native_connection.fetch(statement)
        return [tuple(record.values()) for record in records]  # type: ignore

    async def insert_data(self, table: type[T], data: Iterable[T]) -> None:
        if not is_dataclass_type(table):
            raise TypeError(f"expected dataclass type, got: {table}")
        generator = self.connection.create_generator(table)
        records = generator.get_records_as_tuples(data)
        await self.native_connection.copy_records_to_table(
            table_name=table.__name__,
            columns=tuple(field.name for field in dataclasses.fields(table)),
            records=records,
        )
