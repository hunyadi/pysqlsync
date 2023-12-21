import dataclasses
import logging
import typing
from typing import Any, Iterable, TypeVar

import redshift_connector
from strong_typing.inspection import DataclassInstance, is_dataclass_type

from pysqlsync.base import BaseConnection, BaseContext
from pysqlsync.resultset import resultset_unwrap_object, resultset_unwrap_tuple
from pysqlsync.util.dispatch import thread_dispatch
from pysqlsync.util.typing import override

D = TypeVar("D", bound=DataclassInstance)
T = TypeVar("T")

LOGGER = logging.getLogger("pysqlsync.redshift")

redshift_connector.paramstyle = "numeric"


class RedshiftConnection(BaseConnection):
    native: redshift_connector.Connection

    @override
    @thread_dispatch
    def open(self) -> BaseContext:
        LOGGER.info(f"connecting to {self.params}")
        conn = redshift_connector.connect(
            iam=True,
            host=self.params.host,
            port=self.params.port,
            database=self.params.database,
            db_user=self.params.username,
        )
        conn.autocommit = True

        self.native = conn
        return RedshiftContext(self)

    @override
    @thread_dispatch
    def close(self) -> None:
        self.native.close()


class RedshiftContext(BaseContext):
    def __init__(self, connection: RedshiftConnection) -> None:
        super().__init__(connection)

    @property
    def native_connection(self) -> redshift_connector.Connection:
        return typing.cast(RedshiftConnection, self.connection).native

    @override
    @thread_dispatch
    def _execute(self, statement: str) -> None:
        with self.native_connection.cursor() as cur:
            cur.execute(statement)

    @override
    @thread_dispatch
    def _execute_all(self, statement: str, args: Iterable[tuple[Any, ...]]) -> None:
        with self.native_connection.cursor() as cur:
            cur.executemany(statement, args)

    @override
    @thread_dispatch
    def _query_all(self, signature: type[T], statement: str) -> list[T]:
        with self.native_connection.cursor() as cur:
            records = cur.execute(statement).fetchall()

            if is_dataclass_type(signature):
                return resultset_unwrap_object(signature, records)  # type: ignore
            else:
                return resultset_unwrap_tuple(signature, records)
