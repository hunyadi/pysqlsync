import logging
import typing
from dataclasses import dataclass
from typing import Optional

from ..base import BaseContext, DiscoveryError, Explorer
from ..model.data_types import escape_like, quote
from ..model.id_types import LocalId, QualifiedId, SupportsQualifiedId
from .data_types import SqlDiscovery
from .object_types import (
    Column,
    Constraint,
    ConstraintReference,
    ForeignConstraint,
    Namespace,
    ObjectFactory,
    Table,
    UniqueConstraint,
)

LOGGER = logging.getLogger("pysqlsync")


@dataclass
class AnsiColumnMeta:
    column_name: str
    data_type: str
    nullable: bool
    character_maximum_length: Optional[int]
    numeric_precision: Optional[int]
    numeric_scale: Optional[int]
    datetime_precision: Optional[int]


@dataclass
class AnsiUniqueMeta:
    constraint_name: str
    table_schema: str
    table_name: str
    column_name: str


@dataclass
class AnsiConstraintMeta:
    fk_constraint_name: str
    fk_table_schema: str
    fk_table_name: str
    fk_column_name: str
    fk_ordinal_position: int
    uq_constraint_name: str
    uq_table_schema: str
    uq_table_name: str
    uq_column_name: str
    uq_ordinal_position: int


class AnsiExplorer(Explorer):
    discovery: SqlDiscovery
    factory: ObjectFactory

    _has_constraints: Optional[bool] = None
    _has_column_extended_info: Optional[bool] = None

    def __init__(
        self, conn: BaseContext, discovery: SqlDiscovery, factory: ObjectFactory
    ) -> None:
        super().__init__(conn)
        self.discovery = discovery
        self.factory = factory

    async def has_constraints(self) -> bool:
        "True if `information_schema` has tables to query for referential constraints and key/column usage."

        if self._has_constraints is None:
            try:
                await self.conn.query_one(
                    int,
                    "SELECT COUNT(*) FROM information_schema.table_constraints",
                )
                await self.conn.query_one(
                    int,
                    "SELECT COUNT(*) FROM information_schema.referential_constraints",
                )
                await self.conn.query_one(
                    int,
                    "SELECT COUNT(*) FROM information_schema.key_column_usage",
                )
                LOGGER.info("explorer: PK/FK information available")
                self._has_constraints = True
            except Exception:
                LOGGER.info("explorer: PK/FK information NOT available")
                self._has_constraints = False

        return self._has_constraints

    async def has_column_extended_info(self) -> bool:
        "True if extended information is available for columns in `information_schema`."

        if self._has_column_extended_info is None:
            try:
                await self.conn.query_one(
                    tuple[int, int, int, int, int, int, int],
                    "SELECT\n"
                    "    COUNT(column_name),\n"
                    "    COUNT(data_type),\n"
                    "    COUNT(is_nullable),\n"
                    "    COUNT(character_maximum_length),\n"
                    "    COUNT(numeric_precision),\n"
                    "    COUNT(numeric_scale),\n"
                    "    COUNT(datetime_precision)\n"
                    "FROM information_schema.columns",
                )
                LOGGER.info("explorer: extended column information available")
                self._has_column_extended_info = True
            except Exception:
                LOGGER.info("explorer: extended column information NOT available")
                self._has_column_extended_info = False

        return self._has_column_extended_info

    async def get_table_names(self) -> list[QualifiedId]:
        records = await self.conn.query_all(
            tuple[str, str],
            "SELECT table_schema, table_name\n"
            "FROM information_schema.tables\n"
            "WHERE table_schema != 'information_schema' AND table_schema != 'pg_catalog'\n"
            "ORDER BY table_name ASC",
        )
        return [QualifiedId(record[0], record[1]) for record in records]  # type: ignore[arg-type]

    def _where_table(self, table_id: SupportsQualifiedId, alias: str) -> str:
        conditions: list[str] = []
        conditions.append(f"{alias}.table_name = {quote(table_id.local_id)}")
        if table_id.scope_id is not None:
            conditions.append(f"{alias}.table_schema = {quote(table_id.scope_id)}")
        return " AND ".join(f"({c})" for c in conditions)

    async def has_table(self, table_id: SupportsQualifiedId) -> bool:
        "Checks if a table exists."

        count = await self.conn.query_one(
            int,
            f"SELECT COUNT(*) FROM information_schema.tables AS tab WHERE {self._where_table(table_id, 'tab')}",
        )
        return count > 0

    async def has_column(
        self, table_id: SupportsQualifiedId, column_id: LocalId
    ) -> bool:
        "Checks if a table has the specified column."

        count = await self.conn.query_one(
            int,
            "SELECT COUNT(*)\n"
            "FROM information_schema.columns AS col\n"
            f"WHERE {self._where_table(table_id, 'col')} AND col.column_name = {quote(column_id.local_id)}",
        )
        return count > 0

    async def get_columns(self, table_id: SupportsQualifiedId) -> list[Column]:
        if await self.has_column_extended_info():
            return await self._get_columns_full(table_id)
        else:
            return await self._get_columns_limited(table_id)

    async def _get_columns_limited(self, table_id: SupportsQualifiedId) -> list[Column]:
        column_meta = await self.conn.query_all(
            tuple[str, str, bool],
            "SELECT col.column_name, col.data_type, CASE WHEN col.is_nullable = 'YES' THEN 1 ELSE 0 END AS nullable\n"
            "FROM information_schema.columns AS col\n"
            f"WHERE {self._where_table(table_id, 'col')}\n"
            "ORDER BY ordinal_position",
        )

        columns: list[Column] = []
        for col in column_meta:
            column_name, data_type, nullable = typing.cast(
                tuple[str, str, bool],
                col,
            )
            columns.append(
                self.factory.column_class(
                    LocalId(column_name),
                    self.discovery.sql_data_type_from_spec(type_name=data_type),
                    bool(nullable),
                )
            )
        return columns

    async def _get_columns_full(self, table_id: SupportsQualifiedId) -> list[Column]:
        column_meta = await self.conn.query_all(
            AnsiColumnMeta,
            "SELECT\n"
            "    column_name AS column_name,\n"
            "    data_type AS data_type,\n"
            "    CASE WHEN is_nullable = 'YES' THEN 1 ELSE 0 END AS nullable,\n"
            "    character_maximum_length AS character_maximum_length,\n"
            "    numeric_precision AS numeric_precision,\n"
            "    numeric_scale AS numeric_scale,\n"
            "    datetime_precision AS datetime_precision\n"
            "FROM information_schema.columns AS col\n"
            f"WHERE {self._where_table(table_id, 'col')}\n"
            "ORDER BY ordinal_position",
        )

        columns: list[Column] = []
        for col in column_meta:
            columns.append(
                self.factory.column_class(
                    LocalId(col.column_name),
                    self.discovery.sql_data_type_from_spec(
                        type_name=col.data_type,
                        character_maximum_length=col.character_maximum_length,
                        numeric_precision=col.numeric_precision,
                        numeric_scale=col.numeric_scale,
                        datetime_precision=col.datetime_precision,
                    ),
                    bool(col.nullable),
                )
            )
        return columns

    async def get_table_primary_key(self, table_id: SupportsQualifiedId) -> LocalId:
        primary_meta = await self.conn.query_all(
            str,
            "SELECT\n"
            "    kcu.column_name\n"
            "FROM information_schema.table_constraints AS tab\n"
            "    INNER JOIN information_schema.key_column_usage AS kcu\n"
            "        ON tab.constraint_catalog = kcu.constraint_catalog AND tab.constraint_schema = kcu.constraint_schema AND tab.constraint_name = kcu.constraint_name\n"
            f"WHERE {self._where_table(table_id, 'tab')} AND {self._where_table(table_id, 'kcu')} AND\n"
            "    tab.constraint_type = 'PRIMARY KEY'\n"
            "ORDER BY kcu.ordinal_position",
        )

        if len(primary_meta) < 1:
            raise DiscoveryError(f"primary key required in table: {table_id}")
        elif len(primary_meta) > 1:
            raise DiscoveryError(f"composite primary key in table: {table_id}")
        return LocalId(primary_meta[0])

    async def get_unique_constraints(
        self, table_id: SupportsQualifiedId
    ) -> list[UniqueConstraint]:
        constraint_meta = await self.conn.query_all(
            AnsiUniqueMeta,
            "SELECT\n"
            "    kcu.constraint_name AS constraint_name,\n"
            "    kcu.table_schema AS table_schema,\n"
            "    kcu.table_name AS table_name,\n"
            "    kcu.column_name AS column_name\n"
            "FROM information_schema.table_constraints AS tab\n"
            "    INNER JOIN information_schema.key_column_usage AS kcu\n"
            "        ON tab.constraint_catalog = kcu.constraint_catalog AND tab.constraint_schema = kcu.constraint_schema AND tab.constraint_name = kcu.constraint_name\n"
            f"WHERE {self._where_table(table_id, 'tab')} AND {self._where_table(table_id, 'kcu')} AND\n"
            "    tab.constraint_type = 'UNIQUE'\n"
            "ORDER BY kcu.ordinal_position",
        )

        constraints: dict[str, UniqueConstraint] = {}
        for con in constraint_meta:
            if con.constraint_name in constraints:
                raise DiscoveryError(f"composite unique key in table: {table_id}")
            constraints[con.constraint_name] = UniqueConstraint(
                LocalId(con.constraint_name), LocalId(con.column_name)
            )
        return list(constraints.values())

    async def get_referential_constraints(
        self, table_id: SupportsQualifiedId
    ) -> list[ForeignConstraint]:
        constraint_meta = await self.conn.query_all(
            AnsiConstraintMeta,
            "SELECT\n"
            "    kcu1.constraint_name AS fk_constraint_name,\n"
            "    kcu1.table_schema AS fk_table_schema,\n"
            "    kcu1.table_name AS fk_table_name,\n"
            "    kcu1.column_name AS fk_column_name,\n"
            "    kcu1.ordinal_position AS fk_ordinal_position,\n"
            "    kcu2.constraint_name AS uq_constraint_name,\n"
            "    kcu2.table_schema AS uq_table_schema,\n"
            "    kcu2.table_name AS uq_table_name,\n"
            "    kcu2.column_name AS uq_column_name,\n"
            "    kcu2.ordinal_position AS uq_ordinal_position\n"
            "FROM information_schema.referential_constraints AS ref\n"
            "    INNER JOIN information_schema.key_column_usage AS kcu1\n"
            "        ON kcu1.constraint_catalog = ref.constraint_catalog AND kcu1.constraint_schema = ref.constraint_schema AND kcu1.constraint_name = ref.constraint_name\n"
            "    INNER JOIN information_schema.key_column_usage AS kcu2\n"
            "        ON kcu2.constraint_catalog = ref.unique_constraint_catalog AND kcu2.constraint_schema = ref.unique_constraint_schema AND kcu2.constraint_name = ref.unique_constraint_name\n"
            f"WHERE {self._where_table(table_id, 'kcu1')} AND\n"
            "    kcu1.ordinal_position = kcu2.ordinal_position\n"
            "ORDER BY kcu1.ordinal_position",
        )

        constraints: dict[str, ForeignConstraint] = {}
        for con in constraint_meta:
            if con.fk_constraint_name in constraints:
                raise DiscoveryError(f"composite foreign key in table: {table_id}")
            constraints[con.fk_constraint_name] = ForeignConstraint(
                LocalId(con.fk_constraint_name),
                LocalId(con.fk_column_name),
                ConstraintReference(
                    QualifiedId(con.uq_table_schema, con.uq_table_name),
                    LocalId(con.uq_column_name),
                ),
            )
        return list(constraints.values())

    async def get_table_description(
        self, table_id: SupportsQualifiedId
    ) -> Optional[str]:
        return None

    async def get_table(self, table_id: SupportsQualifiedId) -> Table:
        if not await self.has_table(table_id):
            raise DiscoveryError(f"table not found: {table_id}")

        columns = await self.get_columns(table_id)

        if await self.has_constraints():
            primary_key = await self.get_table_primary_key(table_id)
            constraints: list[Constraint] = []
            constraints.extend(await self.get_unique_constraints(table_id))
            constraints.extend(await self.get_referential_constraints(table_id))
            description = await self.get_table_description(table_id)
            return self.factory.table_class(
                name=table_id,
                columns=columns,
                primary_key=primary_key,
                constraints=constraints or None,
                description=description,
            )
        else:
            # assume first column is the primary key
            primary_key = columns[0].name
            return self.factory.table_class(
                name=table_id, columns=columns, primary_key=primary_key
            )

    async def get_namespace(self, namespace_id: LocalId) -> Namespace:
        tables: list[Table] = []

        # create namespace using qualified IDs
        table_names = await self.conn.query_all(
            str,
            "SELECT table_name\n"
            "FROM information_schema.tables\n"
            f"WHERE table_schema = {quote(namespace_id.id)}\n"
            "ORDER BY table_name ASC",
        )
        if table_names:
            for table_name in table_names:
                table = await self.get_table(
                    self.get_qualified_id(namespace_id.local_id, table_name)
                )
                tables.append(table)

            return self.factory.namespace_class(
                name=namespace_id, enums=[], structs=[], tables=tables
            )

        # create namespace using flat IDs
        table_names = await self.conn.query_all(
            str,
            "SELECT table_name\n"
            "FROM information_schema.tables\n"
            f"WHERE table_name LIKE '{escape_like(namespace_id.id, '~')}~_~_%' ESCAPE '~'\n"
            "ORDER BY table_name ASC",
        )
        if table_names:
            for table_name in table_names:
                table_name = table_name.removeprefix(f"{namespace_id.local_id}__")

                table = await self.get_table(
                    self.get_qualified_id(namespace_id.local_id, table_name)
                )
                tables.append(table)

            return self.factory.namespace_class(
                name=LocalId(""), enums=[], structs=[], tables=tables
            )

        return self.factory.namespace_class()
