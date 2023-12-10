from typing import Optional

from pysqlsync.formation.object_types import (
    Column,
    Namespace,
    ObjectFactory,
    Table,
    join_or_none,
)
from pysqlsync.model.data_types import quote
from pysqlsync.model.id_types import LocalId


class MSSQLColumn(Column):
    @property
    def data_spec(self) -> str:
        nullable = " NOT NULL" if not self.nullable else ""
        identity = " IDENTITY" if self.identity else ""
        return f"{self.data_type}{nullable}{identity}"


class MSSQLTable(Table):
    def alter_table_stmt(self, statements: list[str]) -> str:
        return "\n".join(
            f"ALTER TABLE {self.name} {statement};" for statement in statements
        )

    def add_constraints_stmt(self) -> Optional[str]:
        statements: list[str] = []

        constraints: list[str] = []
        for column in self.columns.values():
            if column.default is None:
                continue
            name = LocalId(f"df_{column.name.local_id}")
            constraints.append(
                f"ADD CONSTRAINT {name} DEFAULT {column.default} FOR {column.name}"
            )
        if constraints:
            statements.append(self.alter_table_stmt(constraints))

        if self.table_constraints:
            statements.append(
                f"ALTER TABLE {self.name} ADD\n"
                + ",\n".join(f"CONSTRAINT {c.spec}" for c in self.table_constraints)
                + ";"
            )

        return join_or_none(statements)

    def drop_constraints_stmt(self) -> Optional[str]:
        statements: list[str] = []

        constraints: list[str] = []
        for column in self.columns.values():
            if column.default is None:
                continue
            name = LocalId(f"df_{column.name.local_id}")
            constraints.append(f"DROP CONSTRAINT {name}")
        if constraints:
            statements.append(self.alter_table_stmt(constraints))

        if self.table_constraints:
            statements.append(
                f"ALTER TABLE {self.name} DROP\n"
                + ",\n".join(f"CONSTRAINT {c.name}" for c in self.table_constraints)
                + "\n;"
            )

        return join_or_none(statements)


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
    def namespace_class(self) -> type[Namespace]:
        return MSSQLNamespace
