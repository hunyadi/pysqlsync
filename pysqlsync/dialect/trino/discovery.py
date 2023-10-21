from pysqlsync.base import BaseContext
from pysqlsync.formation.data_types import SqlDiscovery, SqlDiscoveryOptions
from pysqlsync.formation.discovery import AnsiReflection
from pysqlsync.formation.object_types import ObjectFactory


class TrinoExplorer(AnsiReflection):
    def __init__(self, conn: BaseContext) -> None:
        super().__init__(
            conn,
            SqlDiscovery(SqlDiscoveryOptions(substitutions={})),
            ObjectFactory(),
        )
