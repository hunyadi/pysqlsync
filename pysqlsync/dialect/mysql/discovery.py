from pysqlsync.base import BaseContext
from pysqlsync.formation.data_types import SqlDiscovery, SqlDiscoveryOptions
from pysqlsync.formation.discovery import AnsiReflection
from pysqlsync.model.id_types import PrefixedId, SupportsQualifiedId

from .data_types import MySQLDateTimeType


class MySQLExplorer(AnsiReflection):
    def __init__(self, conn: BaseContext) -> None:
        super().__init__(
            conn,
            SqlDiscovery(
                SqlDiscoveryOptions(substitutions={"datetime": MySQLDateTimeType()})
            ),
        )

    def get_qualified_id(self, namespace: str, id: str) -> SupportsQualifiedId:
        return PrefixedId(namespace, id)
