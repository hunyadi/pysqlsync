from pysqlsync.base import BaseContext
from pysqlsync.formation.data_types import SqlDiscovery, SqlDiscoveryOptions
from pysqlsync.formation.discovery import AnsiExplorer
from pysqlsync.formation.object_types import ObjectFactory


class TrinoExplorer(AnsiExplorer):
    """
    Discovers objects in a database exposed via Trino.

    We recommend setting the following configuration options in Trino:
    * postgresql.array-mapping = AS_ARRAY
    * postgresql.include-system-tables = true
    """

    def __init__(self, conn: BaseContext) -> None:
        super().__init__(
            conn,
            SqlDiscovery(SqlDiscoveryOptions(substitutions={})),
            ObjectFactory(),
        )
