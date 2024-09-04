import dataclasses
import logging
import ssl
import typing
from typing import Iterable, Optional, TypeVar

import asyncpg
from strong_typing.inspection import DataclassInstance, is_dataclass_type

from pysqlsync.base import BaseConnection, BaseContext, ClassRef, DataSource
from pysqlsync.connection import ConnectionSSLMode, create_context
from pysqlsync.formation.object_types import Table
from pysqlsync.model.properties import is_identity_type
from pysqlsync.resultset import resultset_unwrap_dict, resultset_unwrap_tuple
from pysqlsync.util.typing import override

D = TypeVar("D", bound=DataclassInstance)
T = TypeVar("T")

LOGGER = logging.getLogger("pysqlsync.postgres")


class PostgreSQLConnection(BaseConnection):
    native: asyncpg.Connection

    @override
    async def open(self) -> BaseContext:
        LOGGER.info("connecting to %s", self.params)

        ssl_mode = self.params.ssl
        if ssl_mode is None or ssl_mode is ConnectionSSLMode.disable:
            return await self._open()
        elif ssl_mode is ConnectionSSLMode.prefer:
            try:
                return await self._open(create_context(ssl_mode))
            except ConnectionError:
                return await self._open()
        elif ssl_mode is ConnectionSSLMode.allow:
            try:
                return await self._open()
            except ConnectionError:
                return await self._open(create_context(ssl_mode))
        elif (
            ssl_mode is ConnectionSSLMode.require
            or ssl_mode is ConnectionSSLMode.verify_ca
            or ssl_mode is ConnectionSSLMode.verify_full
        ):
            return await self._open(create_context(ssl_mode))
        else:
            raise ValueError(f"unsupported SSL mode: {ssl_mode}")

    async def _open(self, ctx: Optional[ssl.SSLContext] = None) -> BaseContext:
        conn = await asyncpg.connect(
            host=self.params.host,
            port=self.params.port,
            user=self.params.username,
            password=self.params.password,
            database=self.params.database,
            ssl=ctx,
        )

        ver = conn.get_server_version()
        LOGGER.info(
            "PostgreSQL version %d.%d.%d %s",
            ver.major,
            ver.minor,
            ver.micro,
            ver.releaselevel,
        )

        self.native = conn
        return PostgreSQLContext(self)

    @override
    async def close(self) -> None:
        await self.native.close()


class PostgreSQLContext(BaseContext):
    def __init__(self, connection: PostgreSQLConnection) -> None:
        super().__init__(connection)

    @property
    def native_connection(self) -> asyncpg.Connection:
        return typing.cast(PostgreSQLConnection, self.connection).native

    @override
    async def _execute(self, statement: str) -> None:
        await self.native_connection.execute(statement)

    @override
    async def _execute_all(self, statement: str, source: DataSource) -> None:
        async for batch in source.batches():
            await self.native_connection.executemany(statement, batch)

    @override
    async def _query_all(self, signature: type[T], statement: str) -> list[T]:
        records: list[asyncpg.Record] = await self.native_connection.fetch(statement)
        if is_dataclass_type(signature):
            return resultset_unwrap_dict(signature, records)  # type: ignore
        else:
            return resultset_unwrap_tuple(signature, records)  # type: ignore

    @override
    async def insert_data(self, table: type[D], data: Iterable[D]) -> None:
        if not is_dataclass_type(table):
            raise TypeError(f"expected dataclass type, got: {table}")
        generator = self.connection.generator
        table_name = generator.get_qualified_id(ClassRef(table))
        records = generator.get_dataclasses_as_records(table, data, skip_identity=True)
        result = await self.native_connection.copy_records_to_table(
            schema_name=table_name.scope_id,
            table_name=table_name.local_id,
            columns=tuple(
                field.name
                for field in dataclasses.fields(table)
                if not is_identity_type(field.type)
            ),
            records=records,
        )
        LOGGER.debug(result)

    @override
    async def _insert_rows(
        self,
        table: Table,
        source: DataSource,
        *,
        field_types: tuple[type, ...],
        field_names: Optional[tuple[str, ...]] = None,
    ) -> None:
        record_generator = await self._generate_records(
            table, source, field_types=field_types, field_names=field_names
        )
        order = tuple(name for name in field_names if name) if field_names else None
        async for batch in record_generator.batches():
            result = await self.native_connection.copy_records_to_table(
                schema_name=table.name.scope_id,
                table_name=table.name.local_id,
                columns=[str(col.name.local_id) for col in table.get_columns(order)],
                records=batch,
            )
            LOGGER.debug(result)
