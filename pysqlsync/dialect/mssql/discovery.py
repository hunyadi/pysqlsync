from typing import Optional

from pysqlsync.base import BaseContext
from pysqlsync.formation.data_types import SqlDiscovery, SqlDiscoveryOptions
from pysqlsync.formation.discovery import AnsiExplorer
from pysqlsync.formation.object_types import Column, Namespace, Table
from pysqlsync.model.data_types import quote
from pysqlsync.model.id_types import LocalId, SupportsQualifiedId

from .data_types import (
    MSSQLBooleanType,
    MSSQLDateTimeType,
    MSSQLEncoding,
    MSSQLVariableCharacterType,
)
from .object_types import MSSQLObjectFactory


class MSSQLExplorer(AnsiExplorer):
    def __init__(self, conn: BaseContext) -> None:
        super().__init__(
            conn,
            SqlDiscovery(
                SqlDiscoveryOptions(
                    substitutions={
                        "bit": MSSQLBooleanType(),
                        "datetime": MSSQLDateTimeType(),
                        "datetime2": MSSQLDateTimeType(),
                        "nvarchar": MSSQLVariableCharacterType(
                            encoding=MSSQLEncoding.UTF16
                        ),
                        "varchar": MSSQLVariableCharacterType(),
                        "text": MSSQLVariableCharacterType(),
                    }
                )
            ),
            MSSQLObjectFactory(),
        )

    async def get_columns(self, table_id: SupportsQualifiedId) -> list[Column]:
        columns = await super().get_columns(table_id)
        identity_columns = await self.conn.query_all(
            str,
            "SELECT c.name\n"
            "FROM sys.schemas AS s\n"
            "    INNER JOIN sys.tables AS t ON t.schema_id = s.schema_id\n"
            "    INNER JOIN sys.columns AS c ON c.object_id = t.object_id\n"
            f"WHERE s.name = {quote(table_id.scope_id or 'dbo')} AND t.name = {quote(table_id.local_id)} AND c.is_identity = 1;",
        )
        default_columns = await self.conn.query_all(
            tuple[str, str],
            "SELECT c.name, d.definition\n"
            "FROM sys.schemas AS s\n"
            "    INNER JOIN sys.tables AS t ON t.schema_id = s.schema_id\n"
            "    INNER JOIN sys.columns AS c ON c.object_id = t.object_id\n"
            "    INNER JOIN sys.default_constraints AS d ON d.object_id = c.default_object_id\n"
            f"WHERE s.name = {quote(table_id.scope_id or 'dbo')} AND t.name = {quote(table_id.local_id)} AND c.default_object_id != 0;",
        )
        default_values = dict(default_columns)
        for c in columns:
            if c.name.id in identity_columns:
                c.identity = True
            if c.name.id in default_values:
                c.default = default_values[c.name.id]

        return columns

    async def get_namespace_current(self) -> Namespace:
        return await self._get_namespace()

    async def get_namespace(self, namespace_id: LocalId) -> Namespace:
        return await self._get_namespace(namespace_id)

    async def _get_namespace(self, namespace_id: Optional[LocalId] = None) -> Namespace:
        tables: list[Table] = []

        # create namespace using qualified IDs
        if namespace_id is not None:
            schema_id = f"SCHEMA_ID({quote(namespace_id.local_id)})"
        else:
            schema_id = "SCHEMA_ID()"
        table_names = await self.conn.query_all(
            str,
            "SELECT name\n"
            "FROM sys.tables\n"
            f"WHERE schema_id = {schema_id} AND is_ms_shipped = 0\n"
            "ORDER BY name ASC",
        )
        if table_names:
            if namespace_id is not None:
                scope_id = namespace_id.local_id
            else:
                scope_id = None

            for table_name in table_names:
                table = await self.get_table(
                    self.get_qualified_id(scope_id, table_name)
                )
                tables.append(table)

            return self.factory.namespace_class(
                name=LocalId(scope_id or ""), enums=[], structs=[], tables=tables
            )
        else:
            return self.factory.namespace_class()
