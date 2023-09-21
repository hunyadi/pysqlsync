from typing import Optional

from ..base import Explorer
from ..model.data_types import sql_data_type_from_spec
from .object_types import (
    Column,
    Constraint,
    ConstraintReference,
    ForeignConstraint,
    LocalId,
    QualifiedId,
    Table,
    quote,
)


class AnsiReflection(Explorer):
    async def get_table_names(self) -> list[QualifiedId]:
        records = await self.conn.query_all(
            tuple[str, str],
            "SELECT table_schema, table_name\n"
            "FROM information_schema.tables\n"
            "WHERE table_schema != 'information_schema' AND table_schema != 'pg_catalog'\n"
            "ORDER BY table_name ASC",
        )
        return [QualifiedId(record[0], record[1]) for record in records]  # type: ignore[arg-type]

    async def has_information_table(self, table_name: str) -> bool:
        "Checks if an information schema table is available."

        return await self.has_table(QualifiedId("information_schema", table_name))

    async def has_information_tables(self, table_names: list[str]) -> bool:
        for table_name in table_names:
            result = await self.has_information_table(table_name)
            if not result:
                return False

        return True

    async def has_information_column(self, table_name: str, column_name: str) -> bool:
        "Checks if an information schema table has the given column."

        return await self.has_column(
            QualifiedId("information_schema", table_name), LocalId(column_name)
        )

    async def has_information_columns(
        self, table_name: str, column_names: list[str]
    ) -> bool:
        for column_name in column_names:
            result = await self.has_information_column(table_name, column_name)
            if not result:
                return False

        return True

    async def has_table(self, table_id: QualifiedId) -> bool:
        "Checks if a table exists."

        conditions: list[str] = []
        conditions.append(f"table_name = {quote(table_id.local_id)}")
        if table_id.scope_id is not None:
            conditions.append(f"table_schema = {quote(table_id.scope_id)}")
        condition = " AND ".join(f"({c})" for c in conditions)

        records = await self.conn.query_all(
            int,
            f"SELECT COUNT(*) FROM information_schema.tables WHERE {condition}",
        )
        if len(records) == 1:
            return records[0] > 0
        else:
            return False

    async def has_column(self, table_id: QualifiedId, column_id: LocalId) -> bool:
        "Checks if a table has the specified column."

        conditions: list[str] = []
        conditions.append(f"table_name = {quote(table_id.local_id)}")
        if table_id.scope_id is not None:
            conditions.append(f"table_schema = {quote(table_id.scope_id)}")
        condition = " AND ".join(f"({c})" for c in conditions)

        records = await self.conn.query_all(
            int,
            "SELECT COUNT(*)\n"
            "FROM information_schema.columns\n"
            f"WHERE {condition} AND column_name = {quote(column_id.local_id)}",
        )
        if len(records) == 1:
            return records[0] > 0
        else:
            return False

    async def get_table_meta(self, table_id: QualifiedId) -> Table:
        conditions: list[str] = []
        conditions.append(f"col.table_name = {quote(table_id.id)}")
        if table_id.namespace is not None:
            conditions.append(f"col.table_schema = {quote(table_id.namespace)}")
        condition = " AND ".join(f"({c})" for c in conditions)

        columns: list[Column] = []
        if await self.has_information_columns(
            "columns",
            [
                "character_maximum_length",
                "numeric_precision",
                "numeric_scale",
                "datetime_precision",
            ],
        ):
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
                "SELECT\n"
                "    column_name,\n"
                "    data_type,\n"
                "    is_nullable = 'YES' AS nullable,\n"
                "    character_maximum_length,\n"
                "    numeric_precision,\n"
                "    numeric_scale,\n"
                "    datetime_precision\n"
                "FROM information_schema.columns col\n"
                f"WHERE {condition}\n"
                "ORDER BY ordinal_position",
            )

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
                        bool(nullable),
                    )
                )
        else:
            column_records = await self.conn.query_all(
                tuple[str, str, bool],
                "SELECT column_name, data_type, is_nullable = 'YES' AS nullable\n"
                "FROM information_schema.columns col\n"
                f"WHERE {condition}\n"
                "ORDER BY ordinal_position",
            )

            for column_record in column_records:
                (column_name, data_type, nullable) = column_record
                columns.append(
                    Column(
                        LocalId(column_name),
                        sql_data_type_from_spec(data_type),
                        bool(nullable),
                    )
                )

        if await self.has_information_tables(["table_constraints", "key_column_usage"]):
            constraint_records = await self.conn.query_all(
                tuple[str, str, str, str, str, str],
                "SELECT col.constraint_name, tab.constraint_type, col.column_name, col.referenced_table_schema, col.referenced_table_name, col.referenced_column_name\n"
                "FROM information_schema.table_constraints AS tab, information_schema.key_column_usage AS col\n"
                "WHERE col.constraint_schema = tab.constraint_schema AND col.constraint_name = tab.constraint_name\n"
                "AND col.table_schema = tab.table_schema AND col.table_name = tab.table_name\n"
                "AND (tab.constraint_type = 'PRIMARY KEY' OR tab.constraint_type = 'FOREIGN KEY')\n"
                f"AND {condition}\n"
                "ORDER BY col.ordinal_position",
            )

            primary_key: Optional[LocalId] = None
            constraints: dict[str, Constraint] = {}
            for constraint_record in constraint_records:
                (
                    constraint_name,
                    constraint_type,
                    column_name,
                    referenced_table_schema,
                    referenced_table_name,
                    referenced_column_name,
                ) = constraint_record

                if constraint_type == "PRIMARY KEY":
                    if primary_key is not None:
                        raise NotImplementedError(
                            f"composite primary key in table: {table_id}"
                        )
                    primary_key = LocalId(column_name)
                elif constraint_type == "FOREIGN KEY":
                    if constraint_name in constraints:
                        raise NotImplementedError(
                            f"composite foreign key in table: {table_id}"
                        )
                    constraints[constraint_name] = ForeignConstraint(
                        LocalId(constraint_name),
                        LocalId(column_name),
                        ConstraintReference(
                            QualifiedId(referenced_table_schema, referenced_table_name),
                            LocalId(referenced_column_name),
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

        else:
            # assume first column is the primary key
            primary_key = columns[0].name
            return Table(name=table_id, columns=columns, primary_key=primary_key)
