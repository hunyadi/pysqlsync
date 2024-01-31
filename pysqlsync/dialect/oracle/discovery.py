import logging
import re
from dataclasses import dataclass
from typing import Optional

from pysqlsync.base import BaseContext, Explorer
from pysqlsync.formation.constraints import ForeignFactory, UniqueFactory
from pysqlsync.formation.data_types import SqlDiscovery, SqlDiscoveryOptions
from pysqlsync.formation.object_types import Column, Constraint, Namespace, Table
from pysqlsync.model.data_types import escape_like, quote
from pysqlsync.model.id_types import (
    LocalId,
    PrefixedId,
    QualifiedId,
    SupportsQualifiedId,
)

from .data_types import (
    OracleIntegerType,
    OracleTimestampType,
    OracleTimeType,
    OracleVariableBinaryType,
    OracleVariableCharacterType,
)
from .object_types import OracleObjectFactory

LOGGER = logging.getLogger("pysqlsync.oracle")


@dataclass
class OracleColumnMeta:
    column_name: str
    data_type: str
    data_type_owner: str
    data_length: int
    data_precision: int
    data_scale: int
    is_nullable: bool
    data_default: str
    char_length: int
    is_identity: bool
    comments: str


@dataclass
class OracleConstraintMeta:
    constraint_name: str
    constraint_type: str
    source_column: str
    target_table: str
    target_column: str


class OracleExplorer(Explorer):
    discovery: SqlDiscovery
    factory: OracleObjectFactory

    def __init__(self, conn: BaseContext) -> None:
        super().__init__(conn)
        self.discovery = SqlDiscovery(
            SqlDiscoveryOptions(
                substitutions={
                    "BLOB": OracleVariableBinaryType(),
                    "CLOB": OracleVariableCharacterType(),
                    "INTERVAL DAY": OracleTimeType(),
                    "LONG": OracleVariableCharacterType(),
                    "NUMBER": OracleIntegerType(),
                    "RAW": OracleVariableBinaryType(),
                    "TIMESTAMP": OracleTimestampType(),
                    "VARCHAR2": OracleVariableCharacterType(),
                }
            )
        )
        self.factory = OracleObjectFactory()

    def get_qualified_id(
        self, namespace: Optional[str], id: str
    ) -> SupportsQualifiedId:
        return PrefixedId(namespace, id)

    def split_composite_id(self, name: str) -> SupportsQualifiedId:
        if "__" in name:
            parts = name.split("__", 1)
            return PrefixedId(parts[0], parts[1])
        else:
            return PrefixedId(None, name)

    async def get_table_names(self) -> list[QualifiedId]:
        raise NotImplementedError()

    async def has_table(self, table_id: SupportsQualifiedId) -> bool:
        raise NotImplementedError()

    async def has_column(
        self, table_id: SupportsQualifiedId, column_id: LocalId
    ) -> bool:
        raise NotImplementedError()

    async def get_table(self, table_id: SupportsQualifiedId) -> Table:
        condition = f"t.table_name = {quote(table_id.local_id)}"

        table_comments = await self.conn.query_all(
            str,
            "SELECT tc.comments\n"
            "FROM dba_tables t JOIN dba_tab_comments tc ON t.owner = tc.owner AND t.table_name = tc.table_name\n"
            f"WHERE t.owner != 'SYS' AND {condition}\n AND t.tablespace_name != 'SYSAUX'\n"
            "    AND t.status = 'VALID' AND t.temporary = 'N' AND t.secondary = 'N' AND t.dropped = 'NO' AND t.read_only = 'NO'",
        )

        column_records = await self.conn.query_all(
            OracleColumnMeta,
            "SELECT\n"
            "    t.column_name,\n"
            "    t.data_type,\n"
            "    t.data_type_owner,\n"
            "    t.data_length,\n"
            "    t.data_precision,\n"
            "    t.data_scale,\n"
            "    t.nullable != 'N' AS is_nullable,\n"
            "    t.data_default,\n"
            "    t.char_length,\n"
            "    t.identity_column = 'YES' AS is_identity,\n"
            "    tc.comments\n"
            "FROM dba_tab_columns t\n"
            "    JOIN dba_col_comments tc ON t.owner = tc.owner AND t.table_name = tc.table_name AND t.column_name = tc.column_name\n"
            f"WHERE {condition}\n"
            "ORDER BY column_id",
        )

        columns: list[Column] = []
        for col in column_records:
            char_length: Optional[int]
            if col.data_type in ["BLOB", "CLOB"]:
                char_length = None
            else:
                char_length = col.char_length or col.data_length
            m = re.match("^([^()]+)", col.data_type)
            if m is not None:
                data_type = m.group(1)
            else:
                data_type = col.data_type

            data_type = self.discovery.sql_data_type_from_spec(
                type_name=data_type,
                type_schema=col.data_type_owner,
                type_def=col.data_type,
                character_maximum_length=char_length,
                numeric_precision=col.data_precision,
                numeric_scale=col.data_scale,
            )
            nullable = bool(col.is_nullable)
            identity = bool(col.is_identity)
            columns.append(
                self.factory.column_class(
                    LocalId(col.column_name),
                    data_type,
                    nullable,
                    identity=identity,
                    default=col.data_default if not identity else None,
                    description=col.comments,
                )
            )

        constraint_records = await self.conn.query_all(
            OracleConstraintMeta,
            f"SELECT\n"
            "    t.constraint_name AS constraint_name,\n"
            "    t.constraint_type AS constraint_type,\n"
            "    tc.column_name AS source_column,\n"
            "    r.table_name AS target_table,\n"
            "    rc.column_name AS target_column\n"
            "FROM dba_constraints t\n"
            "    JOIN dba_cons_columns tc ON t.owner = tc.owner AND t.constraint_name = tc.constraint_name\n"
            "    LEFT JOIN dba_constraints r ON t.r_owner = r.owner AND t.r_constraint_name = r.constraint_name\n"
            "    LEFT JOIN dba_cons_columns rc ON r.owner = rc.owner AND r.constraint_name = rc.constraint_name\n"
            f"WHERE {condition}\n"
            "ORDER BY tc.position",
        )

        primary: list[LocalId] = []
        unique = UniqueFactory()
        foreign = ForeignFactory()
        for con in constraint_records:
            if con.constraint_type == "P":
                primary.append(LocalId(con.source_column))
            elif con.constraint_type == "U":
                unique.add(con.constraint_name, LocalId(con.source_column))
            elif con.constraint_type == "R":
                foreign.add(
                    con.constraint_name,
                    LocalId(con.source_column),
                    self.split_composite_id(con.target_table),
                    LocalId(con.target_column),
                )

        constraints: list[Constraint] = []
        constraints.extend(unique.fetch())
        constraints.extend(foreign.fetch())

        return self.factory.table_class(
            name=table_id,
            columns=columns,
            primary_key=tuple(primary),
            constraints=constraints or None,
            description=table_comments[0] if table_comments else None,
        )

    async def get_namespace_current(self) -> Namespace:
        return await self._get_namespace()

    async def get_namespace(self, namespace_id: LocalId) -> Namespace:
        return await self._get_namespace(namespace_id)

    async def _get_namespace(self, namespace_id: Optional[LocalId] = None) -> Namespace:
        if namespace_id is not None:
            condition = (
                f"table_name LIKE '{escape_like(namespace_id.id, '~')}~_~_%' ESCAPE '~'"
            )
        else:
            condition = "table_name NOT LIKE '%~_~_%' ESCAPE '~'"

        table_names = await self.conn.query_all(
            str,
            "SELECT table_name FROM dba_tables\n"
            f"WHERE owner != 'SYS' AND {condition}\n AND table_name NOT LIKE '%$%' AND tablespace_name != 'SYSAUX'\n"
            "    AND status = 'VALID' AND temporary = 'N' AND secondary = 'N' AND dropped = 'NO' AND read_only = 'NO'",
        )

        tables: list[Table] = []
        for table_name in table_names:
            table = await self.get_table(self.split_composite_id(table_name))
            tables.append(table)

        if tables:
            return self.factory.namespace_class(enums=[], structs=[], tables=tables)
        else:
            return self.factory.namespace_class()
