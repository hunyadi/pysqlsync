from pysqlsync.base import BaseContext, DiscoveryError
from pysqlsync.formation.data_types import SqlDiscovery, SqlDiscoveryOptions
from pysqlsync.formation.discovery import AnsiConstraintMeta, AnsiReflection
from pysqlsync.formation.object_types import (
    Constraint,
    ConstraintReference,
    ForeignConstraint,
)
from pysqlsync.model.data_types import SqlVariableBinaryType, SqlVariableCharacterType
from pysqlsync.model.id_types import LocalId, PrefixedId, SupportsQualifiedId

from .data_types import (
    MySQLDateTimeType,
    MySQLVariableBinaryType,
    MySQLVariableCharacterType,
)
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
                        "tinyblob": MySQLVariableBinaryType(storage=255),
                        "blob": MySQLVariableBinaryType(storage=65535),
                        "mediumblob": MySQLVariableBinaryType(storage=16777215),
                        "longblob": MySQLVariableBinaryType(storage=4294967295),
                    }
                )
            ),
            MySQLObjectFactory(),
        )

    def get_qualified_id(self, namespace: str, id: str) -> SupportsQualifiedId:
        return PrefixedId(namespace, id)

    def split_composite_id(self, name: str) -> SupportsQualifiedId:
        if "__" in name:
            parts = name.split("__", 2)
            return PrefixedId(parts[0], parts[1])
        else:
            return PrefixedId(None, name)

    async def _get_table_constraints(
        self, table_id: SupportsQualifiedId
    ) -> list[Constraint]:
        constraint_meta = await self.conn.query_all(
            AnsiConstraintMeta,
            "SELECT\n"
            "    kcu.constraint_name AS fk_constraint_name,\n"
            "    kcu.table_schema AS fk_table_schema,\n"
            "    kcu.table_name AS fk_table_name,\n"
            "    kcu.column_name AS fk_column_name,\n"
            "    kcu.ordinal_position AS fk_ordinal_position,\n"
            "    'PRIMARY' AS uq_constraint_name,\n"
            "    kcu.referenced_table_schema AS uq_table_schema,\n"
            "    kcu.referenced_table_name AS uq_table_name,\n"
            "    kcu.referenced_column_name AS uq_column_name,\n"
            "    kcu.ordinal_position AS uq_ordinal_position\n"
            "FROM information_schema.referential_constraints AS ref\n"
            "    INNER JOIN information_schema.key_column_usage AS kcu\n"
            "        ON kcu.constraint_catalog = ref.constraint_catalog AND kcu.constraint_schema = ref.constraint_schema AND kcu.constraint_name = ref.constraint_name\n"
            f"WHERE {self._where_table(table_id, 'ref')} AND {self._where_table(table_id, 'kcu')}\n",
        )

        constraints: dict[str, Constraint] = {}
        for con in constraint_meta:
            if con.fk_constraint_name in constraints:
                raise DiscoveryError(f"composite foreign key in table: {table_id}")
            constraints[con.fk_constraint_name] = ForeignConstraint(
                LocalId(con.fk_constraint_name),
                LocalId(con.fk_column_name),
                ConstraintReference(
                    self.split_composite_id(con.uq_table_name),
                    LocalId(con.uq_column_name),
                ),
            )

        return list(constraints.values())
