from typing import Optional

from pysqlsync.formation.object_types import Column, Namespace, ObjectFactory, Table
from pysqlsync.model.data_types import quote


class MSSQLColumn(Column):
    @property
    def data_spec(self) -> str:
        nullable = " NOT NULL" if not self.nullable else ""
        default = f" DEFAULT {self.default}" if self.default is not None else ""
        identity = " IDENTITY" if self.identity else ""
        return f"{self.data_type}{nullable}{default}{identity}"


class MSSQLTable(Table):
    def alter_table_stmt(self, statements: list[str]) -> str:
        return "\n".join(
            f"ALTER TABLE {self.name} {statement};" for statement in statements
        )

    def add_constraints_stmt(self) -> Optional[str]:
        if self.constraints and any(c.is_alter_table() for c in self.constraints):
            return (
                f"ALTER TABLE {self.name} ADD\n"
                + ",\n".join(
                    f"CONSTRAINT {c.spec}"
                    for c in self.constraints
                    if c.is_alter_table()
                )
                + "\n;"
            )
        else:
            return None

    def drop_constraints_stmt(self) -> Optional[str]:
        if self.constraints and any(c.is_alter_table() for c in self.constraints):
            return (
                f"ALTER TABLE {self.name} DROP\n"
                + ",\n".join(
                    f"CONSTRAINT {c.name}"
                    for c in self.constraints
                    if c.is_alter_table()
                )
                + "\n;"
            )
        else:
            return None


class MSSQLNamespace(Namespace):
    def create_schema_stmt(self) -> str:
        # Microsoft SQL Server requires a separate batch for creating a schema
        return f"IF NOT EXISTS ( SELECT * FROM sys.schemas WHERE name = N{quote(self.name.id)} ) EXEC('CREATE SCHEMA {self.name}');"


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
