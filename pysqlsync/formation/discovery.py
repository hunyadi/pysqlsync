from typing import Optional

from ..base import BaseContext
from ..model.data_types import sql_data_type_from_spec
from .object_types import Column, LocalId, QualifiedId, Table, quote


class Reflection:
    conn: BaseContext

    def __init__(self, conn: BaseContext) -> None:
        self.conn = conn

    async def get_table_names(self) -> list[QualifiedId]:
        records = await self.conn.query_all(
            tuple[str, str],
            "SELECT table_schema, table_name "
            "FROM information_schema.tables "
            "WHERE table_schema != 'information_schema' AND table_schema != 'pg_catalog' "
            "ORDER BY table_name ASC",
        )
        return [QualifiedId(record[0], record[1]) for record in records]  # type: ignore[arg-type]

    async def has_information_table(self, table_name: str) -> bool:
        "Checks if an information schema table is available."

        records = await self.conn.query_all(
            tuple[int],
            "SELECT COUNT(*) "
            "FROM information_schema.tables "
            f"WHERE LOWER(table_schema) = 'information_schema' AND LOWER(table_name) = {quote(table_name)}",
        )
        if len(records) == 1:
            (value,) = records[0]
            return value > 0
        else:
            return False

    async def get_table(self, id: QualifiedId) -> Table:
        conditions: list[str] = []
        conditions.append(f"col.table_name = {quote(id.id)}")
        if id.namespace is not None:
            conditions.append(f"col.table_schema = {quote(id.namespace)}")
        condition = " AND ".join(f"({c})" for c in conditions)

        column_records = await self.conn.query_all(
            tuple[
                str,
                str,
                bool,
                Optional[int],
                Optional[int],
                Optional[int],
                Optional[int],
            ],
            "SELECT column_name, data_type, is_nullable = 'YES' AS nullable, character_maximum_length, numeric_precision, numeric_scale, datetime_precision "
            "FROM information_schema.columns col "
            f"WHERE {condition} "
            "ORDER BY ordinal_position",
        )

        columns: list[Column] = []
        for column_record in column_records:
            (
                column_name,
                data_type,
                nullable,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                timestamp_precision,
            ) = column_record
            columns.append(
                Column(
                    LocalId(column_name),
                    sql_data_type_from_spec(
                        data_type,
                        character_maximum_length=character_maximum_length,
                        numeric_precision=numeric_precision,
                        numeric_scale=numeric_scale,
                        timestamp_precision=timestamp_precision,
                    ),
                    nullable,
                )
            )

        has_table_constraints = await self.has_information_table("table_constraints")
        has_primary_key = await self.has_information_table("key_column_usage")

        if has_table_constraints and has_primary_key:
            primary_records = await self.conn.query_all(
                tuple[str],
                "SELECT col.column_name "
                "FROM information_schema.table_constraints AS tab, information_schema.key_column_usage AS col "
                "WHERE col.constraint_schema = tab.constraint_schema AND col.constraint_name = tab.constraint_name "
                "AND col.table_schema = tab.table_schema AND col.table_name = tab.table_name "
                "AND tab.constraint_type = 'PRIMARY KEY' "
                f"AND {condition} "
                "ORDER BY col.ordinal_position",
            )
            primary_keys = [r[0] for r in primary_records]
            primary_key = LocalId(primary_keys[0])
        else:
            # assume first column is the primary key
            primary_key = columns[0].name

        return Table(name=id, columns=columns, primary_key=primary_key)
