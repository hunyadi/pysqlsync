import asyncpg
from typing import Any, Iterable

from ..base import BaseConnection, BaseContext, Parameters


class Connection(BaseConnection):
    conn: asyncpg.Connection

    async def __aenter__(self) -> BaseContext:
        self.conn = await asyncpg.connect(
            host=self.params.host,
            port=self.params.port,
            user=self.params.username,
            password=self.params.password,
            database=self.params.database,
        )
        return Context(self.conn)

    async def __aexit__(self, exc_type, exc, tb):
        await self.conn.close()


class Context(BaseContext):
    conn: asyncpg.Connection

    def __init__(self, conn: asyncpg.Connection) -> None:
        self.conn = conn

    async def execute(self, statement: str) -> None:
        await self.conn.execute(statement)

    async def execute_all(
        self, statement: str, args: Iterable[tuple[Any, ...]]
    ) -> None:
        await self.conn.executemany(statement, args)
