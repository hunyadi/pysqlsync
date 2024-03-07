import re
from typing import Optional

from pysqlsync.formation.object_types import (
    EnumTable,
    Namespace,
    ObjectFactory,
    StructType,
    Table,
)
from pysqlsync.model.id_types import LocalId

_sql_quoted_str_table = str.maketrans(
    {
        "\\": "\\\\",
        "'": "\\'",
        "\b": "\\b",
        "\f": "\\f",
        "\n": "\\n",
        "\r": "\\r",
        "\t": "\\t",
    }
)


def sql_quoted_string(text: str) -> str:
    if re.search(r"[\b\f\n\r\t]", text):
        string = text.translate(_sql_quoted_str_table)
        return f"E'{string}'"
    else:
        string = text.replace("'", "''")
        return f"'{string}'"


class PostgreSQLTable(Table):
    def create_stmt(self) -> str:
        statements: list[str] = []
        statements.append(super().create_stmt())

        # output comments for table and column objects
        if self.description is not None:
            statements.append(
                f"COMMENT ON TABLE {self.name} IS {sql_quoted_string(self.description)};"
            )
        for column in self.columns.values():
            if column.description is not None:
                statements.append(
                    f"COMMENT ON COLUMN {self.name}.{column.name} IS {sql_quoted_string(column.description)};"
                )
        return "\n".join(statements)

    @property
    def primary_key_constraint_id(self) -> LocalId:
        return LocalId(f"pk_{self.name.local_id.replace('.', '_')}")


class PostgreSQLEnumTable(EnumTable, PostgreSQLTable):
    pass


class PostgreSQLStructType(StructType):
    def create_stmt(self) -> str:
        statements: list[str] = []
        statements.append(super().create_stmt())

        if self.description is not None:
            statements.append(
                f"COMMENT ON TYPE {self.name} IS {sql_quoted_string(self.description)};"
            )
        for member in self.members.values():
            if member.description is not None:
                statements.append(
                    f"COMMENT ON COLUMN {self.name}.{member.name} IS {sql_quoted_string(member.description)};"
                )
        return "\n".join(statements)


class PostgreSQLNamespace(Namespace):
    def create_schema_stmt(self) -> Optional[str]:
        if self.name.local_id:
            return f"CREATE SCHEMA IF NOT EXISTS {self.name};"
        else:
            return None

    def drop_schema_stmt(self) -> Optional[str]:
        if self.name.local_id:
            return f"DROP SCHEMA IF EXISTS {self.name} CASCADE;"
        else:
            return None


class PostgreSQLObjectFactory(ObjectFactory):
    @property
    def table_class(self) -> type[Table]:
        return PostgreSQLTable

    @property
    def enum_table_class(self) -> type[EnumTable]:
        return PostgreSQLEnumTable

    @property
    def struct_class(self) -> type[StructType]:
        return PostgreSQLStructType

    @property
    def namespace_class(self) -> type[Namespace]:
        return PostgreSQLNamespace
