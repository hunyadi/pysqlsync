import random
import string
from dataclasses import dataclass
from typing import Optional, Union

from pysqlsync.formation.object_types import (
    Column,
    EnumTable,
    Namespace,
    ObjectFactory,
    Table,
    join_or_none,
)
from pysqlsync.model.data_types import SqlDataType, quote
from pysqlsync.model.id_types import LocalId
from pysqlsync.util.typing import override

ID_GENERATOR = random.Random()


@dataclass
class MSSQLDefault:
    """
    Default value constraint.

    :param name: Globally unique name for the default constraint.
    :param expr: SQL expression for the default value.
    """

    name: str
    expr: str


class MSSQLColumn(Column):
    """
    A column in a Microsoft SQL Server database table.

    :param default_constraint_name: The name of the constraint for DEFAULT.
    """

    default_constraint_name: LocalId

    def __init__(
        self,
        name: LocalId,
        data_type: SqlDataType,
        nullable: bool,
        default: Union[MSSQLDefault, str, None] = None,
        identity: bool = False,
        description: Optional[str] = None,
    ) -> None:
        super().__init__(
            name,
            data_type,
            nullable=nullable,
            default=default.expr if isinstance(default, MSSQLDefault) else default,
            identity=identity,
            description=description,
        )
        if isinstance(default, MSSQLDefault):
            self.default_constraint_name = LocalId(default.name)
        else:
            r = "".join(ID_GENERATOR.choices(string.ascii_lowercase, k=6))
            self.default_constraint_name = LocalId(f"df_{self.name.local_id}_{r}")

    @property
    def data_spec(self) -> str:
        nullable = " NOT NULL" if not self.nullable else ""
        name = self.default_constraint_name
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
            name = self.default_constraint_name
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
