from dataclasses import dataclass
from typing import Optional

from pysqlsync.formation.object_types import (
    Column,
    EnumTable,
    FormationError,
    ObjectFactory,
    Table,
)
from pysqlsync.model.data_types import SqlEnumType, quote
from pysqlsync.model.id_types import LocalId


class MySQLTable(Table):
    @property
    def short_description(self) -> Optional[str]:
        if self.description is None:
            return None

        return (
            self.description
            if "\n" not in self.description
            else self.description[: self.description.index("\n")]
        )

    def create_stmt(self) -> str:
        defs: list[str] = []
        defs.extend(str(c) for c in self.columns.values())
        primary_columns = ", ".join(str(key) for key in self.primary_key)
        defs.append(
            f"CONSTRAINT {self.primary_key_constraint_id} PRIMARY KEY ({primary_columns})"
        )
        definition = ",\n".join(defs)
        comment = (
            f"\nCOMMENT = {quote(self.short_description)}"
            if self.short_description
            else ""
        )
        return f"CREATE TABLE {self.name} (\n{definition}\n){comment};"


class MySQLEnumTable(EnumTable, MySQLTable):
    pass


@dataclass(eq=True)
class MySQLColumn(Column):
    @property
    def data_spec(self) -> str:
        charset = (
            " CHARACTER SET ascii COLLATE ascii_bin"
            if isinstance(self.data_type, SqlEnumType)
            else ""
        )
        nullable = " NOT NULL" if not self.nullable else ""
        default = f" DEFAULT {self.default}" if self.default is not None else ""
        identity = " AUTO_INCREMENT" if self.identity else ""
        description = f" COMMENT {self.comment}" if self.description is not None else ""
        return f"{self.data_type}{charset}{nullable}{default}{identity}{description}"

    @property
    def comment(self) -> Optional[str]:
        if self.description is not None:
            description = (
                self.description
                if "\n" not in self.description
                else self.description[: self.description.index("\n")]
            )

            if len(description) > 1024:
                raise FormationError(
                    f"comment for column {self.name} too long, expected: maximum 1024; got: {len(description)}"
                )

            return quote(description)
        else:
            return None

    def rename_stmt(self, name: str) -> str:
        return f"CHANGE {self.name} {LocalId(name)} {self.data_spec}"


class MySQLObjectFactory(ObjectFactory):
    @property
    def column_class(self) -> type[Column]:
        return MySQLColumn

    @property
    def table_class(self) -> type[Table]:
        return MySQLTable

    @property
    def enum_table_class(self) -> type[EnumTable]:
        return MySQLEnumTable
