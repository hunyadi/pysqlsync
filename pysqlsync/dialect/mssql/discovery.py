from pysqlsync.base import BaseContext
from pysqlsync.formation.data_types import SqlDiscovery, SqlDiscoveryOptions
from pysqlsync.formation.discovery import AnsiExplorer
from pysqlsync.formation.object_types import Column
from pysqlsync.model.data_types import quote
from pysqlsync.model.id_types import SupportsQualifiedId

from .data_types import MSSQLBooleanType, MSSQLDateTimeType, MSSQLVariableCharacterType
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
        for c in columns:
            if c.name.id in identity_columns:
                c.identity = True

        return columns
