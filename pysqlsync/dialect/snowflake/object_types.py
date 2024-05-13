import re
from typing import Optional

from pysqlsync.formation.object_types import Column, ObjectFactory, Table
from pysqlsync.model.id_types import LocalId

_sql_quoted_str_table = str.maketrans(
    {
        "\\": "\\\\",
        "'": "\\'",
        '"': '\\"',
        "\0": "\\0",
        "\b": "\\b",
        "\f": "\\f",
        "\n": "\\n",
        "\r": "\\r",
        "\t": "\\t",
    }
)


def sql_quoted_string(text: str) -> str:
    if re.search(r"[\\'\"\0\b\f\n\r\t]", text):
        text = text.translate(_sql_quoted_str_table)
    return f"'{text}'"


class SnowflakeTable(Table):
    def create_stmt(self) -> str:
        statements: list[str] = []
        statements.append(super().create_stmt())

        # output comments for table objects
        if self.description is not None:
            statements.append(
                f"COMMENT ON TABLE {self.name} IS {sql_quoted_string(self.description)};"
            )
        return "\n".join(statements)

    @property
    def primary_key_constraint_id(self) -> LocalId:
        return LocalId(f"pk_{self.name.local_id.replace('.', '_')}")


class SnowflakeColumn(Column):
    @property
    def data_spec(self) -> str:
        nullable = " NOT NULL" if not self.nullable and not self.identity else ""
        default = f" DEFAULT {self.default}" if self.default is not None else ""
        identity = " IDENTITY" if self.identity else ""
        description = f" COMMENT {self.comment}" if self.description is not None else ""
        return f"{self.data_type}{nullable}{default}{identity}{description}"

    @property
    def comment(self) -> Optional[str]:
        if self.description is not None:
            return sql_quoted_string(self.description)
        else:
            return None


class SnowflakeObjectFactory(ObjectFactory):
    @property
    def column_class(self) -> type[Column]:
        return SnowflakeColumn

    @property
    def table_class(self) -> type[Table]:
        return SnowflakeTable
