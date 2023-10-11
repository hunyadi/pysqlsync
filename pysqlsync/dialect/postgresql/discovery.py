from dataclasses import dataclass
from itertools import groupby
from typing import Optional

from pysqlsync.base import BaseContext, Explorer
from pysqlsync.formation.data_types import SqlDiscovery
from pysqlsync.formation.discovery import DiscoveryError
from pysqlsync.formation.object_types import (
    Column,
    Constraint,
    ConstraintReference,
    EnumType,
    ForeignConstraint,
    Namespace,
    Table,
    quote,
)
from pysqlsync.model.id_types import LocalId, QualifiedId, SupportsQualifiedId

from .generator import PostgreSQLTable


@dataclass
class PostgreSQLTableMeta:
    description: str


@dataclass
class PostgreSQLColumnMeta:
    column_name: str
    type_schema: str
    type_name: str
    nullable: bool
    character_maximum_length: int
    numeric_precision: int
    numeric_scale: int
    datetime_precision: int
    description: str


@dataclass
class PostgreSQLConstraintMeta:
    constraint_type: str
    constraint_name: str
    source_column: str
    target_namespace: str
    target_table: str
    target_column: str


@dataclass
class PostgreSQLEnumMeta:
    enum_name: str
    enum_value: str


class PostgreSQLExplorer(Explorer):
    discovery: SqlDiscovery

    def __init__(self, conn: BaseContext) -> None:
        super().__init__(conn)
        self.discovery = SqlDiscovery()

    async def get_table_names(self) -> list[QualifiedId]:
        rows = await self.conn.query_all(
            tuple[str, str],
            "SELECT nsp.nspname, cls.relname\n"
            "FROM pg_catalog.pg_class AS cls INNER JOIN pg_catalog.pg_namespace AS nsp ON cls.relnamespace = nsp.oid\n"
            f"WHERE cls.relkind = 'r' OR cls.relkind = 'v'",
        )
        return [QualifiedId(row[0], row[1]) for row in rows]  # type: ignore

    @staticmethod
    def _where_table(table_id: SupportsQualifiedId) -> str:
        conditions: list[str] = []
        conditions.append(f"cls.relname = {quote(table_id.local_id)}")
        if table_id.scope_id is not None:
            conditions.append(f"nsp.nspname = {quote(table_id.scope_id)}")
        conditions.append("cls.relkind = 'r' OR cls.relkind = 'v'")
        return " AND ".join(f"({c})" for c in conditions)

    async def has_table(self, table_id: SupportsQualifiedId) -> bool:
        rows = await self.conn.query_all(
            int,
            "SELECT COUNT(*)\n"
            "FROM pg_catalog.pg_class AS cls INNER JOIN pg_catalog.pg_namespace AS nsp ON cls.relnamespace = nsp.oid\n"
            f"WHERE {self._where_table(table_id)}",
        )
        return len(rows) > 0 and rows[0] > 0

    async def has_column(
        self, table_id: SupportsQualifiedId, column_id: LocalId
    ) -> bool:
        conditions: list[str] = []
        conditions.append(f"cls.relname = {quote(table_id.local_id)}")
        if table_id.scope_id is not None:
            conditions.append(f"nsp.nspname = {quote(table_id.scope_id)}")
        conditions.append("cls.relkind = 'r' OR cls.relkind = 'v'")
        conditions.append(f"att.attname = {quote(column_id.local_id)}")
        condition = " AND ".join(f"({c})" for c in conditions)

        rows = await self.conn.query_all(
            int,
            "SELECT COUNT(*)\n"
            "FROM pg_catalog.pg_attribute AS att\n"
            "    INNER JOIN pg_catalog.pg_class AS cls ON att.attrelid = cls.oid\n"
            "    INNER JOIN pg_catalog.pg_namespace AS nsp ON cls.relnamespace = nsp.oid\n"
            f"WHERE {condition}",
        )
        return len(rows) > 0 and rows[0] > 0

    async def get_table_meta(self, table_id: SupportsQualifiedId) -> Table:
        table_records = await self.conn.query_one(
            PostgreSQLTableMeta,
            "SELECT\n"
            "    dsc.description AS description\n"
            "FROM pg_catalog.pg_class AS cls\n"
            "    INNER JOIN pg_catalog.pg_namespace AS nsp ON cls.relnamespace = nsp.oid\n"
            "    LEFT JOIN pg_catalog.pg_description AS dsc ON dsc.objoid = cls.oid AND dsc.objsubid = 0\n"
            f"WHERE {self._where_table(table_id)}",
        )

        column_records = await self.conn.query_all(
            PostgreSQLColumnMeta,
            "SELECT\n"
            "    att.attname AS column_name,\n"
            "    typ_nsp.nspname AS type_schema,\n"
            "    typ.typname AS type_name,\n"
            "    NOT att.attnotnull AS nullable,\n"
            "    col.character_maximum_length AS character_maximum_length,\n"
            "    col.numeric_precision AS numeric_precision,\n"
            "    col.numeric_scale AS numeric_scale,\n"
            "    col.datetime_precision AS datetime_precision,\n"
            "    dsc.description AS description\n"
            "FROM pg_catalog.pg_attribute AS att\n"
            "    INNER JOIN pg_catalog.pg_type AS typ ON att.atttypid = typ.oid\n"
            "    INNER JOIN pg_catalog.pg_namespace AS typ_nsp ON typ.typnamespace = typ_nsp.oid\n"
            "    INNER JOIN pg_catalog.pg_class AS cls ON att.attrelid = cls.oid\n"
            "    INNER JOIN pg_catalog.pg_namespace AS nsp ON cls.relnamespace = nsp.oid\n"
            "    LEFT JOIN pg_catalog.pg_description AS dsc ON dsc.objoid = cls.oid AND dsc.objsubid = att.attnum\n"
            "    INNER JOIN information_schema.columns col ON nsp.nspname = col.table_schema AND cls.relname = col.table_name AND att.attname = col.column_name\n"
            f"WHERE {self._where_table(table_id)} AND (att.attnum > 0)\n"
            "ORDER BY att.attnum",
        )

        columns: list[Column] = []
        for col in column_records:
            columns.append(
                Column(
                    LocalId(col.column_name),
                    self.discovery.sql_data_type_from_spec(
                        col.type_name,
                        col.type_schema,
                        character_maximum_length=col.character_maximum_length,
                        numeric_precision=col.numeric_precision,
                        numeric_scale=col.numeric_scale,
                        datetime_precision=None,
                    ),
                    bool(col.nullable),
                    description=col.description,
                )
            )

        constraint_records = await self.conn.query_all(
            PostgreSQLConstraintMeta,
            "SELECT\n"
            "    cons.contype AS constraint_type,\n"
            "    cons.conname AS constraint_name,\n"
            "    att_s.attname AS source_column,\n"
            "    nsp_t.nspname AS target_namespace,\n"
            "    cls_t.relname AS target_table,\n"
            "    att_t.attname AS target_column\n"
            "FROM (\n"
            "SELECT\n"
            "    con.contype,\n"
            "    con.conname,\n"
            "    con.conrelid AS pkeyrel,\n"
            "    tgt.pkeycol,\n"
            "    con.confrelid AS fkeyrel,\n"
            "    tgt.fkeycol\n"
            "FROM pg_catalog.pg_constraint AS con\n"
            "    CROSS JOIN UNNEST(con.conkey, con.confkey) AS tgt(pkeycol, fkeycol)\n"
            "    INNER JOIN pg_catalog.pg_class AS cls ON con.conrelid = cls.oid\n"
            "    INNER JOIN pg_catalog.pg_namespace AS nsp ON cls.relnamespace = nsp.oid\n"
            f"WHERE {self._where_table(table_id)} AND con.contype IN ('p', 'f')\n"
            ") AS cons\n"
            "    INNER JOIN pg_catalog.pg_attribute AS att_s ON cons.pkeyrel = att_s.attrelid AND cons.pkeycol = att_s.attnum\n"
            "    LEFT JOIN pg_catalog.pg_class AS cls_t ON cons.fkeyrel = cls_t.oid\n"
            "    LEFT JOIN pg_catalog.pg_namespace AS nsp_t ON cls_t.relnamespace = nsp_t.oid\n"
            "    LEFT JOIN pg_catalog.pg_attribute AS att_t ON cons.fkeyrel = att_t.attrelid AND cons.fkeycol = att_t.attnum",
        )

        primary_key: Optional[LocalId] = None
        constraints: dict[str, Constraint] = {}
        for con in constraint_records:
            if con.constraint_type == b"p":
                if primary_key is not None:
                    raise NotImplementedError(
                        f"composite primary key in table: {table_id}"
                    )
                primary_key = LocalId(con.source_column)
            elif con.constraint_type == b"f":
                if con.constraint_name in constraints:
                    raise NotImplementedError(
                        f"composite foreign key in table: {table_id}"
                    )
                constraints[con.constraint_name] = ForeignConstraint(
                    LocalId(con.constraint_name),
                    LocalId(con.source_column),
                    ConstraintReference(
                        QualifiedId(con.target_namespace, con.target_table),
                        LocalId(con.target_column),
                    ),
                )

        if primary_key is None:
            raise DiscoveryError(f"primary key required in table: {table_id}")

        return PostgreSQLTable(
            name=table_id,
            columns=columns,
            primary_key=primary_key,
            constraints=list(constraints.values()) or None,
            description=table_records.description,
        )

    async def get_namespace_meta(self, namespace_id: LocalId) -> Namespace:
        enum_records = await self.conn.query_all(
            PostgreSQLEnumMeta,
            "SELECT\n"
            "t.typname as enum_name,\n"
            "e.enumlabel as enum_value\n"
            "FROM pg_catalog.pg_type t\n"
            "    JOIN pg_catalog.pg_enum e ON t.oid = e.enumtypid\n"
            "    JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace\n"
            f"WHERE n.nspname = {quote(namespace_id.id)}"
            "ORDER BY enum_name, e.enumsortorder\n"
            ";",
        )

        enums: list[EnumType] = []
        for enum_name, enum_groups in groupby(enum_records, key=lambda e: e.enum_name):
            enums.append(
                EnumType(
                    QualifiedId(namespace_id.id, enum_name),
                    [e.enum_value for e in enum_groups],
                )
            )

        table_names = await self.conn.query_all(
            str,
            "SELECT cls.relname\n"
            "FROM pg_catalog.pg_class AS cls INNER JOIN pg_catalog.pg_namespace AS nsp ON cls.relnamespace = nsp.oid\n"
            f"WHERE nsp.nspname = {quote(namespace_id.id)} AND (cls.relkind = 'r' OR cls.relkind = 'v')\n"
            ";",
        )

        tables: list[Table] = []
        for table_name in table_names:
            table = await self.get_table_meta(QualifiedId(namespace_id.id, table_name))
            tables.append(table)

        if tables:
            return Namespace(namespace_id, enums=enums, structs=[], tables=tables)
        else:
            return Namespace()
