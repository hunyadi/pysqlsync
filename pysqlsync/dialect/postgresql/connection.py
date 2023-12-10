import dataclasses
import logging
import typing
from typing import Any, Iterable, Optional, TypeVar

import asyncpg
from strong_typing.inspection import DataclassInstance, is_dataclass_type

from pysqlsync.base import BaseConnection, BaseContext, ClassRef
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
        LOGGER.info(f"connecting to {self.params}")
        conn = await asyncpg.connect(
            host=self.params.host,
            port=self.params.port,
            user=self.params.username,
            password=self.params.password,
            database=self.params.database,
        )

        ver = conn.get_server_version()
        LOGGER.info(
            f"PostgreSQL version {ver.major}.{ver.minor}.{ver.micro} {ver.releaselevel}"
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
    async def _execute_all(
        self, statement: str, args: Iterable[tuple[Any, ...]]
    ) -> None:
        await self.native_connection.executemany(statement, args)

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
        records: Iterable[tuple[Any, ...]],
        *,
        field_types: tuple[type, ...],
        field_names: Optional[tuple[str, ...]] = None,
    ) -> None:
        record_generator = await self._generate_records(
            table, records, field_types=field_types, field_names=field_names
        )
        order = tuple(name for name in field_names if name) if field_names else None
        result = await self.native_connection.copy_records_to_table(
            schema_name=table.name.scope_id,
            table_name=table.name.local_id,
            columns=[str(col.name.local_id) for col in table.get_columns(order)],
            records=record_generator,
        )
        LOGGER.debug(result)
