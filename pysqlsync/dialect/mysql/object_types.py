import typing
from dataclasses import dataclass
from typing import Optional

from pysqlsync.formation.object_types import (
    Column,
    FormationError,
    MutableObject,
    ObjectFactory,
    Table,
)
from pysqlsync.model.data_types import SqlEnumType, quote


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
        defs.append(f"PRIMARY KEY ({self.primary_key})")
        definition = ",\n".join(defs)
        comment = (
            f"\nCOMMENT = {quote(self.short_description)}"
            if self.short_description
            else ""
        )
        return f"CREATE TABLE {self.name} (\n{definition}\n){comment};"

    def mutate_stmt(self, src: MutableObject) -> Optional[str]:
        statements: list[str] = []
        stmt = super().mutate_stmt(src)
        if stmt is not None:
            statements.append(stmt)

        source = typing.cast(MySQLTable, src)
        target = self

        source_desc = source.short_description
        target_desc = target.short_description

        if source_desc is None:
            if target_desc is not None:
                statements.append(
                    f"ALTER TABLE {self.name} COMMENT = {quote(target_desc)};"
                )
        else:
            if target_desc is None:
                statements.append(f"ALTER TABLE {self.name} COMMENT = {quote('')};")
            elif source_desc != target_desc:
                statements.append(
                    f"ALTER TABLE {self.name} COMMENT = {quote(target_desc)};"
                )

        return "\n".join(statements) if statements else None


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

    def mutate_column_stmt(self, src: Column) -> list[str]:
        source = typing.cast(MySQLColumn, src)
        target = self
        statements: list[str] = []
        if (
            source.data_type != target.data_type
            or source.nullable != target.nullable
            or source.default != target.default
            or source.identity != target.identity
            or source.comment != target.comment
        ):
            statements.append(f"MODIFY COLUMN {source.name} {target.data_spec}")
        return statements

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


class MySQLObjectFactory(ObjectFactory):
    @property
    def column_class(self) -> type[Column]:
        return MySQLColumn

    @property
    def table_class(self) -> type[Table]:
        return MySQLTable
