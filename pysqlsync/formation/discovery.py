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
    Table,
)


@dataclass
class AnsiColumnMeta:
    column_name: str
    column_type: str
    data_type: str
    nullable: bool
    character_maximum_length: Optional[int]
    numeric_precision: Optional[int]
    numeric_scale: Optional[int]
    datetime_precision: Optional[int]


class AnsiReflection(Explorer):
    discovery: SqlDiscovery

    def __init__(self, conn: BaseContext, discovery: SqlDiscovery) -> None:
        super().__init__(conn)
        self.discovery = discovery

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

    async def has_table(self, table_id: SupportsQualifiedId) -> bool:
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

    async def has_column(
        self, table_id: SupportsQualifiedId, column_id: LocalId
    ) -> bool:
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

    async def get_table_meta(self, table_id: SupportsQualifiedId) -> Table:
        conditions: list[str] = []
        conditions.append(f"col.table_name = {quote(table_id.local_id)}")
        if table_id.scope_id is not None:
            conditions.append(f"col.table_schema = {quote(table_id.scope_id)}")
        condition = " AND ".join(f"({c})" for c in conditions)

        columns: list[Column] = []
        if await self.has_information_columns(
            "columns",
            [
                "column_type",
                "character_maximum_length",
                "numeric_precision",
                "numeric_scale",
                "datetime_precision",
            ],
        ):
            ansi_column_records = await self.conn.query_all(
                AnsiColumnMeta,
                "SELECT\n"
                "    column_name AS column_name,\n"
                "    column_type AS column_type,\n"
                "    data_type AS data_type,\n"
                "    is_nullable = 'YES' AS nullable,\n"
                "    character_maximum_length AS character_maximum_length,\n"
                "    numeric_precision AS numeric_precision,\n"
                "    numeric_scale AS numeric_scale,\n"
                "    datetime_precision AS datetime_precision\n"
                "FROM information_schema.columns col\n"
                f"WHERE {condition}\n"
                "ORDER BY ordinal_position",
            )

            for col in ansi_column_records:
                columns.append(
                    self.conn.connection.generator.column_class(
                        LocalId(col.column_name),
                        self.discovery.sql_data_type_from_spec(
                            col.column_type,
                            character_maximum_length=col.character_maximum_length,
                            numeric_precision=col.numeric_precision,
                            numeric_scale=col.numeric_scale,
                            datetime_precision=col.datetime_precision,
                        ),
                        bool(col.nullable),
                    )
                )
        else:
            limited_column_records = await self.conn.query_all(
                tuple[str, str, bool],
                "SELECT column_name, data_type, is_nullable = 'YES' AS nullable\n"
                "FROM information_schema.columns col\n"
                f"WHERE {condition}\n"
                "ORDER BY ordinal_position",
            )

            for column_record in limited_column_records:
                column_name, data_type, nullable = typing.cast(
                    tuple[str, str, bool],
                    column_record,
                )
                columns.append(
                    self.conn.connection.generator.column_class(
                        LocalId(column_name),
                        self.discovery.sql_data_type_from_spec(data_type),
                        bool(nullable),
                    )
                )

        if not columns:
            raise DiscoveryError(f"table not found: {table_id}")

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
                ) = typing.cast(tuple[str, str, str, str, str, str], constraint_record)

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
                raise DiscoveryError(f"primary key required in table: {table_id}")

            return self.conn.connection.generator.table_class(
                name=table_id,
                columns=columns,
                primary_key=primary_key,
                constraints=list(constraints.values()) or None,
            )

        else:
            # assume first column is the primary key
            primary_key = columns[0].name
            return self.conn.connection.generator.table_class(
                name=table_id, columns=columns, primary_key=primary_key
            )

    async def get_namespace_meta(self, namespace_id: LocalId) -> Namespace:
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
                table = await self.get_table_meta(
                    self.get_qualified_id(namespace_id.local_id, table_name)
                )
                tables.append(table)

            return Namespace(name=namespace_id, enums=[], structs=[], tables=tables)

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

                table = await self.get_table_meta(
                    self.get_qualified_id(namespace_id.local_id, table_name)
                )
                tables.append(table)

            return Namespace(name=LocalId(""), enums=[], structs=[], tables=tables)

        return Namespace()
