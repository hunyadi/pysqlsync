from pysqlsync.base import BaseContext
from pysqlsync.formation.data_types import SqlDiscovery, SqlDiscoveryOptions
from pysqlsync.formation.discovery import AnsiReflection
from pysqlsync.model.data_types import SqlVariableBinaryType, SqlVariableCharacterType
from pysqlsync.model.id_types import PrefixedId, SupportsQualifiedId

from .data_types import MySQLDateTimeType, MySQLVariableCharacterType
from .object_types import MySQLObjectFactory


class MySQLExplorer(AnsiReflection):
    def __init__(self, conn: BaseContext) -> None:
        super().__init__(
            conn,
            SqlDiscovery(
                SqlDiscoveryOptions(
                    substitutions={
                        "datetime": MySQLDateTimeType(),
                        "tinytext": MySQLVariableCharacterType(limit=255),
                        "text": MySQLVariableCharacterType(limit=65535),
                        "mediumtext": MySQLVariableCharacterType(limit=16777215),
                        "longtext": MySQLVariableCharacterType(limit=4294967295),
                        "tinyblob": SqlVariableBinaryType(storage=255),
                        "blob": SqlVariableBinaryType(storage=65535),
                        "mediumblob": SqlVariableBinaryType(storage=16777215),
                        "longblob": SqlVariableBinaryType(storage=4294967295),
                    }
                )
            ),
            MySQLObjectFactory(),
        )

    def get_qualified_id(self, namespace: str, id: str) -> SupportsQualifiedId:
        return PrefixedId(namespace, id)
