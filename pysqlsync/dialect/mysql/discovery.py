from dataclasses import dataclass
from typing import Optional

from pysqlsync.base import BaseContext
from pysqlsync.formation.constraints import ForeignFactory
from pysqlsync.formation.data_types import SqlDiscovery, SqlDiscoveryOptions
from pysqlsync.formation.discovery import (
    AnsiColumnMeta,
    AnsiConstraintMeta,
    AnsiExplorer,
)
from pysqlsync.formation.object_types import Column, ForeignConstraint, Namespace
from pysqlsync.model.data_types import quote
from pysqlsync.model.id_types import LocalId, PrefixedId, SupportsQualifiedId
from pysqlsync.util.typing import override

from .data_types import (
    MySQLDateTimeType,
    MySQLVariableBinaryType,
    MySQLVariableCharacterType,
)
from .object_types import MySQLObjectFactory


@dataclass
class MySQLColumnMeta(AnsiColumnMeta):
    column_type: str
    extra: str
    column_comment: str


class MySQLExplorer(AnsiExplorer):
    def __init__(self, conn: BaseContext) -> None:
        super().__init__(
            conn,
            SqlDiscovery(
                SqlDiscoveryOptions(
                    substitutions={
                        "datetime": MySQLDateTimeType(),
                        "varchar": MySQLVariableCharacterType(),
                        "tinytext": MySQLVariableCharacterType(limit=255),
                        "text": MySQLVariableCharacterType(limit=65535),
                        "mediumtext": MySQLVariableCharacterType(limit=16777215),
                        "longtext": MySQLVariableCharacterType(limit=4294967295),
                        "varbinary": MySQLVariableBinaryType(),
                        "tinyblob": MySQLVariableBinaryType(storage=255),
                        "blob": MySQLVariableBinaryType(storage=65535),
                        "mediumblob": MySQLVariableBinaryType(storage=16777215),
                        "longblob": MySQLVariableBinaryType(storage=4294967295),
                    }
                )
            ),
            MySQLObjectFactory(),
        )

    @override
    def get_qualified_id(
        self, namespace: Optional[str], id: str
    ) -> SupportsQualifiedId:
        return PrefixedId(namespace, id)

    def split_composite_id(self, name: str) -> SupportsQualifiedId:
        if "__" in name:
            parts = name.split("__", 1)
            return PrefixedId(parts[0], parts[1])
        else:
            return PrefixedId(None, name)

    async def get_columns(self, table_id: SupportsQualifiedId) -> list[Column]:
        column_meta = await self.conn.query_all(
            MySQLColumnMeta,
            "SELECT\n"
            "    column_name AS column_name,\n"
            "    data_type AS data_type,\n"
            "    CASE WHEN is_nullable = 'YES' THEN 1 ELSE 0 END AS nullable,\n"
            "    column_default AS column_default,\n"
            "    character_maximum_length AS character_maximum_length,\n"
            "    numeric_precision AS numeric_precision,\n"
            "    numeric_scale AS numeric_scale,\n"
            "    datetime_precision AS datetime_precision,\n"
            "    column_type AS column_type,\n"
            "    extra AS extra,\n"
            "    column_comment AS column_comment\n"
            "FROM information_schema.columns AS col\n"
            f"WHERE {self._where_table(table_id, 'col')}\n"
            "ORDER BY ordinal_position",
        )

        columns: list[Column] = []
        for col in column_meta:
            columns.append(
                self.factory.column_class(
                    LocalId(col.column_name),
                    self.discovery.sql_data_type_from_spec(
                        type_name=col.data_type,
                        type_def=col.column_type,
                        character_maximum_length=col.character_maximum_length,
                        numeric_precision=col.numeric_precision,
                        numeric_scale=col.numeric_scale,
                        datetime_precision=col.datetime_precision,
                    ),
                    bool(col.nullable),
                    default=col.column_default,
                    identity="auto_increment" in col.extra,
                    description=col.column_comment or None,
                )
            )
        return columns

    async def get_referential_constraints(
        self, table_id: SupportsQualifiedId
    ) -> list[ForeignConstraint]:
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
            "        ON kcu.constraint_catalog = ref.constraint_catalog\n"
            "            AND kcu.constraint_schema = ref.constraint_schema\n"
            "            AND kcu.constraint_name = ref.constraint_name\n"
            f"WHERE ref.table_name = {quote(table_id.local_id)} AND {self._where_table(table_id, 'kcu')}\n",
        )

        constraints = ForeignFactory()
        for con in constraint_meta:
            constraints.add(
                con.fk_constraint_name,
                LocalId(con.fk_column_name),
                self.split_composite_id(con.uq_table_name),
                LocalId(con.uq_column_name),
            )
        return constraints.fetch()

    async def get_table_description(
        self, table_id: SupportsQualifiedId
    ) -> Optional[str]:
        return (
            await self.conn.query_one(
                str,
                "SELECT table_comment\n"
                "FROM information_schema.tables AS tab\n"
                f"WHERE {self._where_table(table_id, 'tab')}",
            )
            or None
        )

    async def get_namespace_current(self) -> Namespace:
        return await self.get_namespace_flat()

    async def get_namespace(self, namespace_id: LocalId) -> Namespace:
        return await self.get_namespace_flat(namespace_id)
