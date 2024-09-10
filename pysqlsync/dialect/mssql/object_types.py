from typing import Optional

from pysqlsync.formation.object_types import (
    Column,
    EnumTable,
    Namespace,
    ObjectFactory,
    Table,
    join_or_none,
)
from pysqlsync.model.data_types import quote
from pysqlsync.model.id_types import LocalId
from pysqlsync.util.typing import override


class MSSQLColumn(Column):
    def default_constraint_name(self) -> LocalId:
        "The name of the constraint for DEFAULT."

        return LocalId(f"df_{self.name.local_id}")

    @property
    def data_spec(self) -> str:
        nullable = " NOT NULL" if not self.nullable else ""
        name = self.default_constraint_name()
        default = (
            f" CONSTRAINT {name} DEFAULT {self.default}"
            if self.default is not None
            else ""
        )
        identity = " IDENTITY" if self.identity else ""
        return f"{self.data_type}{nullable}{default}{identity}"

    @override
    def create_stmt(self) -> str:
        return f"ADD {self.column_spec}"

    @override
    def drop_stmt(self) -> str:
        if self.default is not None:
            name = self.default_constraint_name()
            return f"DROP CONSTRAINT {name}, COLUMN {self.name}"
        else:
            return f"DROP COLUMN {self.name}"


class MSSQLTable(Table):
    def alter_table_stmt(self, statements: list[str]) -> str:
        return "\n".join(
            f"ALTER TABLE {self.name} {statement};" for statement in statements
        )

    def add_constraints_stmt(self) -> Optional[str]:
        statements: list[str] = []
        if self.table_constraints:
            statements.append(
                f"ALTER TABLE {self.name} ADD\n"
                + ",\n".join(f"CONSTRAINT {c.spec}" for c in self.table_constraints)
                + ";"
            )
        return join_or_none(statements)

    def drop_constraints_stmt(self) -> Optional[str]:
        statements: list[str] = []
        if self.table_constraints:
            statements.append(
                f"ALTER TABLE {self.name} DROP\n"
                + ",\n".join(f"CONSTRAINT {c.name}" for c in self.table_constraints)
                + "\n;"
            )

        return join_or_none(statements)


class MSSQLEnumTable(EnumTable, MSSQLTable):
    pass


class MSSQLNamespace(Namespace):
    def create_schema_stmt(self) -> Optional[str]:
        if self.name.local_id:
            # Microsoft SQL Server requires a separate batch for creating a schema
            return f"IF NOT EXISTS ( SELECT * FROM sys.schemas WHERE name = N{quote(self.name.id)} ) EXEC('CREATE SCHEMA {self.name}');"
        else:
            return None


class MSSQLObjectFactory(ObjectFactory):
    @property
    def column_class(self) -> type[Column]:
        return MSSQLColumn

    @property
    def table_class(self) -> type[Table]:
        return MSSQLTable

    @property
    def enum_table_class(self) -> type[EnumTable]:
        return MSSQLEnumTable

    @property
    def namespace_class(self) -> type[Namespace]:
        return MSSQLNamespace
