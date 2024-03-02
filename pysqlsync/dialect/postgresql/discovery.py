import datetime
import re
from dataclasses import dataclass
from itertools import groupby
from typing import Optional

from pysqlsync.base import BaseContext, Explorer
from pysqlsync.formation.constraints import ForeignFactory, UniqueFactory
from pysqlsync.formation.data_types import SqlDiscovery, SqlDiscoveryOptions
from pysqlsync.formation.object_types import (
    Column,
    Constraint,
    EnumType,
    Namespace,
    StructMember,
    StructType,
    Table,
)
from pysqlsync.model.data_types import SqlArrayType, SqlUserDefinedType, quote
from pysqlsync.model.id_types import GlobalId, LocalId, QualifiedId, SupportsQualifiedId

from .data_types import PostgreSQLJsonType
from .object_types import PostgreSQLObjectFactory


def to_default_expr(expr: Optional[str]) -> Optional[str]:
    "Converts a PostgreSQL-specific default expression into a standard default expression."

    if expr is None:
        return None

    m = re.match(
        r"^'(?P<year>\d{4})-(?P<mon>\d{2})-(?P<day>\d{2}) (?P<hour>\d{2}):(?P<min>\d{2}):(?P<sec>\d{2})'::timestamp without time zone$",
        expr,
    )
    if m:
        timestamp = datetime.datetime(
            int(m.group("year")),
            int(m.group("mon")),
            int(m.group("day")),
            int(m.group("hour")),
            int(m.group("min")),
            int(m.group("sec")),
            tzinfo=None,
        )
        return quote(timestamp.isoformat(sep=" "))
    else:
        return expr


@dataclass
class PostgreSQLStructMeta:
    description: str


@dataclass
class PostgreSQLMemberMeta:
    member_name: str
    type_schema: str
    type_name: str
    type_def: str
    description: str


@dataclass
class PostgreSQLTableMeta:
    description: str


@dataclass
class PostgreSQLColumnMeta:
    column_name: str
    type_schema: str
    type_name: str
    type_def: str
    is_nullable: bool
    is_array: bool
    is_identity: bool
    default_value: str
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
    factory: PostgreSQLObjectFactory

    def __init__(self, conn: BaseContext) -> None:
        super().__init__(conn)
        self.discovery = SqlDiscovery(
            SqlDiscoveryOptions(
                substitutions={
                    "jsonb": PostgreSQLJsonType(),
                    "inet": SqlUserDefinedType(GlobalId("inet")),
                }
            )
        )
        self.factory = PostgreSQLObjectFactory()

    async def get_table_names(self) -> list[QualifiedId]:
        rows = await self.conn.query_all(
            tuple[str, str],
            "SELECT nsp.nspname, cls.relname\n"
            "FROM pg_catalog.pg_class AS cls INNER JOIN pg_catalog.pg_namespace AS nsp ON cls.relnamespace = nsp.oid\n"
            "WHERE cls.relkind = 'r' OR cls.relkind = 'v'",
        )
        return [QualifiedId(row[0], row[1]) for row in rows]  # type: ignore

    @classmethod
    def _where_relation(cls, relation_id: SupportsQualifiedId, kinds: list[str]) -> str:
        conditions: list[str] = []
        conditions.append(f"cls.relname = {quote(relation_id.local_id)}")
        if relation_id.scope_id is not None:
            conditions.append(f"nsp.nspname = {quote(relation_id.scope_id)}")
        conditions.append(
            " OR ".join(f"(cls.relkind = {quote(kind)})" for kind in kinds)
        )
        return " AND ".join(f"({c})" for c in conditions)

    @classmethod
    def _where_struct(cls, struct_id: SupportsQualifiedId) -> str:
        return cls._where_relation(struct_id, ["c"])

    @classmethod
    def _where_table(cls, table_id: SupportsQualifiedId) -> str:
        return cls._where_relation(table_id, ["r", "v"])

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

    async def get_struct(self, struct_id: SupportsQualifiedId) -> StructType:
        conditions: list[str] = []
        conditions.append(f"typ.typname = {quote(struct_id.local_id)}")
        if struct_id.scope_id is not None:
            conditions.append(f"nsp.nspname = {quote(struct_id.scope_id)}")
        condition = " AND ".join(f"({c})" for c in conditions)

        struct_record = await self.conn.query_one(
            PostgreSQLStructMeta,
            "SELECT\n"
            "    dsc.description AS description\n"
            "FROM pg_catalog.pg_type AS typ\n"
            "    INNER JOIN pg_catalog.pg_namespace AS nsp ON typ.typnamespace = nsp.oid\n"
            "    LEFT JOIN pg_catalog.pg_description AS dsc ON dsc.objoid = typ.oid AND dsc.objsubid = 0\n"
            f"WHERE {condition}",
        )

        member_records = await self.conn.query_all(
            PostgreSQLMemberMeta,
            "SELECT\n"
            "    att.attname AS member_name,\n"
            "    typ_nsp.nspname AS type_schema,\n"
            "    typ.typname AS type_name,\n"
            "    CASE WHEN typ.typelem != 0 THEN typ_elem.typname ELSE typ.typname END AS type_name,\n"
            "    pg_catalog.format_type(att.atttypid, att.atttypmod) AS type_def,\n"
            "    dsc.description AS description\n"
            "FROM pg_catalog.pg_attribute AS att\n"
            "    INNER JOIN pg_catalog.pg_type AS typ ON att.atttypid = typ.oid\n"
            "    INNER JOIN pg_catalog.pg_namespace AS typ_nsp ON typ.typnamespace = typ_nsp.oid\n"
            "    INNER JOIN pg_catalog.pg_class AS cls ON att.attrelid = cls.oid\n"
            "    INNER JOIN pg_catalog.pg_namespace AS nsp ON cls.relnamespace = nsp.oid\n"
            "    LEFT JOIN pg_catalog.pg_type AS typ_elem ON typ.typelem = typ_elem.oid\n"
            "    LEFT JOIN pg_catalog.pg_description AS dsc ON dsc.objoid = cls.oid AND dsc.objsubid = att.attnum\n"
            f"WHERE {self._where_struct(struct_id)} AND (att.attnum > 0)\n"
            "ORDER BY att.attnum",
        )

        members: list[StructMember] = []
        for mem in member_records:
            data_type = self.discovery.sql_data_type_from_spec(
                type_name=mem.type_name,
                type_schema=mem.type_schema,
                type_def=mem.type_def,
            )
            members.append(
                StructMember(
                    LocalId(mem.member_name),
                    data_type,
                    description=mem.description or None,
                )
            )

        return self.factory.struct_class(
            name=struct_id,
            members=members,
            description=struct_record.description,
        )

    async def get_table(self, table_id: SupportsQualifiedId) -> Table:
        table_record = await self.conn.query_one(
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
            "    CASE WHEN typ.typelem != 0 THEN typ_elem.typname ELSE typ.typname END AS type_name,\n"
            "    pg_catalog.format_type(att.atttypid, att.atttypmod) AS type_def,\n"
            "    NOT att.attnotnull AS is_nullable,\n"
            "    att.attndims != 0 AS is_array,\n"
            "    att.attidentity IN ('a', 'd') AS is_identity,\n"
            "    pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS default_value,\n"
            "    dsc.description AS description\n"
            "FROM pg_catalog.pg_attribute AS att\n"
            "    INNER JOIN pg_catalog.pg_type AS typ ON att.atttypid = typ.oid\n"
            "    INNER JOIN pg_catalog.pg_namespace AS typ_nsp ON typ.typnamespace = typ_nsp.oid\n"
            "    INNER JOIN pg_catalog.pg_class AS cls ON att.attrelid = cls.oid\n"
            "    INNER JOIN pg_catalog.pg_namespace AS nsp ON cls.relnamespace = nsp.oid\n"
            "    LEFT JOIN pg_catalog.pg_type AS typ_elem ON typ.typelem = typ_elem.oid\n"
            "    LEFT JOIN pg_catalog.pg_attrdef AS def ON def.adrelid = cls.oid AND def.adnum = att.attnum\n"
            "    LEFT JOIN pg_catalog.pg_description AS dsc ON dsc.objoid = cls.oid AND dsc.objsubid = att.attnum\n"
            f"WHERE {self._where_table(table_id)} AND (att.attnum > 0)\n"
            "ORDER BY att.attnum",
        )

        columns: list[Column] = []
        for col in column_records:
            data_type = self.discovery.sql_data_type_from_spec(
                type_name=col.type_name,
                type_schema=col.type_schema,
                type_def=col.type_def,
            )
            if col.is_array:
                data_type = SqlArrayType(data_type)
            columns.append(
                self.factory.column_class(
                    LocalId(col.column_name),
                    data_type,
                    bool(col.is_nullable),
                    identity=col.is_identity,
                    default=to_default_expr(col.default_value),
                    description=col.description or None,
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
            f"WHERE {self._where_table(table_id)} AND con.contype IN ('p', 'u', 'f')\n"
            ") AS cons\n"
            "    INNER JOIN pg_catalog.pg_attribute AS att_s ON cons.pkeyrel = att_s.attrelid AND cons.pkeycol = att_s.attnum\n"
            "    LEFT JOIN pg_catalog.pg_class AS cls_t ON cons.fkeyrel = cls_t.oid\n"
            "    LEFT JOIN pg_catalog.pg_namespace AS nsp_t ON cls_t.relnamespace = nsp_t.oid\n"
            "    LEFT JOIN pg_catalog.pg_attribute AS att_t ON cons.fkeyrel = att_t.attrelid AND cons.fkeycol = att_t.attnum",
        )

        primary: list[LocalId] = []
        unique = UniqueFactory()
        foreign = ForeignFactory()
        for con in constraint_records:
            if con.constraint_type == b"p":
                primary.append(LocalId(con.source_column))
            elif con.constraint_type == b"u":
                unique.add(con.constraint_name, LocalId(con.source_column))
            elif con.constraint_type == b"f":
                foreign.add(
                    con.constraint_name,
                    LocalId(con.source_column),
                    QualifiedId(con.target_namespace, con.target_table),
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
            description=table_record.description,
        )

    async def get_namespace_current(self) -> Namespace:
        schema = await self.conn.current_schema()
        if schema is None:
            raise RuntimeError("unable to fetch current schema")
        return await self.get_namespace(LocalId(schema))

    async def get_namespace(self, namespace_id: LocalId) -> Namespace:
        enum_records = await self.conn.query_all(
            PostgreSQLEnumMeta,
            "SELECT\n"
            "t.typname as enum_name,\n"
            "e.enumlabel as enum_value\n"
            "FROM pg_catalog.pg_type t\n"
            "    JOIN pg_catalog.pg_enum e ON t.oid = e.enumtypid\n"
            "    JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace\n"
            f"WHERE n.nspname = {quote(namespace_id.id)}\n"
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

        struct_names = await self.conn.query_all(
            str,
            "SELECT cls.relname\n"
            "FROM pg_catalog.pg_class AS cls INNER JOIN pg_catalog.pg_namespace AS nsp ON cls.relnamespace = nsp.oid\n"
            f"WHERE nsp.nspname = {quote(namespace_id.id)} AND (cls.relkind = 'c')\n"
            ";",
        )

        structs: list[StructType] = []
        for struct_name in struct_names:
            struct = await self.get_struct(QualifiedId(namespace_id.id, struct_name))
            structs.append(struct)

        table_names = await self.conn.query_all(
            str,
            "SELECT cls.relname\n"
            "FROM pg_catalog.pg_class AS cls INNER JOIN pg_catalog.pg_namespace AS nsp ON cls.relnamespace = nsp.oid\n"
            f"WHERE nsp.nspname = {quote(namespace_id.id)} AND (cls.relkind = 'r' OR cls.relkind = 'v')\n"
            ";",
        )

        tables: list[Table] = []
        for table_name in table_names:
            table = await self.get_table(QualifiedId(namespace_id.id, table_name))
            tables.append(table)

        if enums or structs or tables:
            return self.factory.namespace_class(
                namespace_id, enums=enums, structs=structs, tables=tables
            )
        else:
            return self.factory.namespace_class()
