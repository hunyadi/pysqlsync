import re

from pysqlsync.formation.object_types import Column, EnumTable, ObjectFactory, Table
from pysqlsync.model.data_types import SqlTimestampType


class OracleColumn(Column):
    @property
    def default_expr(self) -> str:
        if self.default is None:
            raise ValueError("default value is NULL")

        if isinstance(self.data_type, SqlTimestampType):
            m = re.match(
                r"^'(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2}) (?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})'$",
                self.default,
            )
            if m:
                return f"TIMESTAMP {self.default}"

        return self.default

    @property
    def data_spec(self) -> str:
        default = f" DEFAULT {self.default_expr}" if self.default is not None else ""
        nullable = " NOT NULL" if not self.nullable and not self.identity else ""
        identity = " GENERATED BY DEFAULT AS IDENTITY" if self.identity else ""
        return f"{self.data_type}{default}{nullable}{identity}"


class OracleTable(Table):
    def alter_table_stmt(self, statements: list[str]) -> str:
        return "\n".join(
            f"ALTER TABLE {self.name} {statement};" for statement in statements
        )

    def drop_if_exists_stmt(self) -> str:
        return (
            "BEGIN\n"
            f"    EXECUTE IMMEDIATE 'DROP TABLE {self.name} CASCADE CONSTRAINTS PURGE';\n"
            "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;\n"
            "END;"
        )


class OracleEnumTable(EnumTable, OracleTable):
    pass


class OracleObjectFactory(ObjectFactory):
    @property
    def column_class(self) -> type[Column]:
        return OracleColumn

    @property
    def table_class(self) -> type[Table]:
        return OracleTable

    @property
    def enum_table_class(self) -> type[EnumTable]:
        return OracleEnumTable
