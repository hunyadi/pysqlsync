from pysqlsync.base import BaseContext
from pysqlsync.formation.data_types import SqlDiscovery, SqlDiscoveryOptions
from pysqlsync.formation.discovery import AnsiReflection

from .data_types import MSSQLDateTimeType
from .object_types import MSSQLObjectFactory


class MSSQLExplorer(AnsiReflection):
    def __init__(self, conn: BaseContext) -> None:
        super().__init__(
            conn,
            SqlDiscovery(
                SqlDiscoveryOptions(
                    substitutions={
                        "datetime": MSSQLDateTimeType(),
                        "datetime2": MSSQLDateTimeType(),
                    }
                )
            ),
            MSSQLObjectFactory(),
        )
