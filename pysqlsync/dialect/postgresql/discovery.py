from dataclasses import dataclass
from typing import Optional

from pysqlsync.base import Explorer
from pysqlsync.formation.object_types import (
    Column,
    Constraint,
    ConstraintReference,
    ForeignConstraint,
    Table,
    quote,
)
from pysqlsync.model.data_types import sql_data_type_from_spec
from pysqlsync.model.id_types import LocalId, QualifiedId


@dataclass
class PostgreSQLConstraint:
    constraint_type: str
    constraint_name: str
    source_column: str
    target_namespace: str
    target_table: str
    target_column: str


class PostgreSQLExplorer(Explorer):
    async def get_table_names(self) -> list[QualifiedId]:
        rows = await self.conn.query_all(
            tuple[str, str],
            "SELECT nsp.nspname, cls.relname\n"
            "FROM pg_catalog.pg_class AS cls INNER JOIN pg_catalog.pg_namespace AS nsp ON cls.relnamespace = nsp.oid\n"
            f"WHERE cls.relkind = 'r' OR cls.relkind = 'v'",
        )
        return [QualifiedId(row[0], row[1]) for row in rows]  # type: ignore

    @staticmethod
    def _where_table(table_id: QualifiedId) -> str:
        conditions: list[str] = []
        conditions.append(f"cls.relname = {quote(table_id.local_id)}")
        if table_id.scope_id is not None:
            conditions.append(f"nsp.nspname = {quote(table_id.scope_id)}")
        conditions.append("cls.relkind = 'r' OR cls.relkind = 'v'")
        return " AND ".join(f"({c})" for c in conditions)

    async def has_table(self, table_id: QualifiedId) -> bool:
        rows = await self.conn.query_all(
            int,
            "SELECT COUNT(*)\n"
            "FROM pg_catalog.pg_class AS cls INNER JOIN pg_catalog.pg_namespace AS nsp ON cls.relnamespace = nsp.oid\n"
            f"WHERE {self._where_table(table_id)}",
        )
        return len(rows) > 0 and rows[0] > 0

    async def has_column(self, table_id: QualifiedId, column_id: LocalId) -> bool:
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

    async def get_table_meta(self, table_id: QualifiedId) -> Table:
        column_records = await self.conn.query_all(
            tuple[str, str, bool, str],
            "SELECT att.attname, typ.typname, NOT att.attnotnull, dsc.description\n"
            "FROM pg_catalog.pg_attribute AS att\n"
            "    INNER JOIN pg_catalog.pg_type AS typ ON att.atttypid = typ.oid\n"
            "    INNER JOIN pg_catalog.pg_class AS cls ON att.attrelid = cls.oid\n"
            "    INNER JOIN pg_catalog.pg_namespace AS nsp ON cls.relnamespace = nsp.oid\n"
            "    LEFT JOIN pg_catalog.pg_description AS dsc ON dsc.objoid = cls.oid AND dsc.objsubid = att.attnum\n"
            f"WHERE {self._where_table(table_id)} AND (att.attnum > 0)\n"
            "ORDER BY att.attnum",
        )

        columns: list[Column] = []
        for column_record in column_records:
            (column_name, data_type, nullable, description) = column_record
            columns.append(
                Column(
                    LocalId(column_name),  # type: ignore
                    sql_data_type_from_spec(data_type),  # type: ignore
                    bool(nullable),
                    description=description,  # type: ignore
                )
            )

        constraint_records = await self.conn.query_all(
            PostgreSQLConstraint,
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
        for c in constraint_records:
            if c.constraint_type == b"p":
                if primary_key is not None:
                    raise NotImplementedError(
                        f"composite primary key in table: {table_id}"
                    )
                primary_key = LocalId(c.source_column)
            elif c.constraint_type == b"f":
                if c.constraint_name in constraints:
                    raise NotImplementedError(
                        f"composite foreign key in table: {table_id}"
                    )
                constraints[c.constraint_name] = ForeignConstraint(
                    LocalId(c.constraint_name),
                    LocalId(c.source_column),
                    ConstraintReference(
                        QualifiedId(c.target_namespace, c.target_table),
                        LocalId(c.target_column),
                    ),
                )

        if primary_key is None:
            raise RuntimeError(f"primary key required in table: {table_id}")

        return Table(
            name=table_id,
            columns=columns,
            primary_key=primary_key,
            constraints=list(constraints.values()),
        )
