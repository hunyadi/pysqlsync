import enum
from dataclasses import dataclass
from typing import Optional

from ..model.data_types import SqlDataType
from ..model.id_types import LocalId, QualifiedId


def quote(s: str) -> str:
    "Quotes a string to be embedded in an SQL statement."

    return "'" + s.replace("'", "''") + "'"


@dataclass
class EnumType:
    name: QualifiedId
    values: list[str]

    def __init__(
        self, enum: type[enum.Enum], *, namespace: Optional[str] = None
    ) -> None:
        self.name = QualifiedId(namespace, enum.__name__)
        self.values = [e.name for e in enum]

    def __str__(self) -> str:
        vals = ", ".join(quote(val) for val in self.values)
        return f"CREATE TYPE {self.name} AS ENUM ({vals});"


@dataclass
class StructMember:
    name: LocalId
    data_type: SqlDataType
    description: Optional[str] = None

    def __str__(self) -> str:
        return f"{self.name} {self.data_type}"


@dataclass
class StructType:
    name: QualifiedId
    members: list[StructMember]
    description: Optional[str] = None

    def __str__(self) -> str:
        members = ",\n".join(str(m) for m in self.members)
        return f"CREATE TYPE {self.name} AS (\n{members}\n);"


@dataclass
class Column:
    name: LocalId
    data_type: SqlDataType
    nullable: bool
    description: Optional[str] = None

    def __str__(self) -> str:
        if self.nullable:
            return f"{self.name} {self.data_type}"
        else:
            return f"{self.name} {self.data_type} NOT NULL"


@dataclass
class Constraint:
    name: LocalId


@dataclass
class ForeignConstraint(Constraint):
    foreign_column: LocalId
    primary_table: QualifiedId
    primary_column: LocalId

    def __str__(self) -> str:
        return f"CONSTRAINT {self.name} FOREIGN KEY ({self.foreign_column}) REFERENCES {self.primary_table} ({self.primary_column})"


@dataclass
class CheckConstraint(Constraint):
    condition: str

    def __str__(self) -> str:
        return f"CONSTRAINT {self.name} CHECK ({self.condition})"


@dataclass
class Table:
    name: QualifiedId
    columns: list[Column]
    primary_key: LocalId
    constraints: Optional[list[Constraint]] = None
    description: Optional[str] = None

    def __str__(self) -> str:
        defs: list[str] = []
        defs.extend(str(c) for c in self.columns)
        defs.append(f"PRIMARY KEY ({self.primary_key})")
        if self.constraints is not None:
            defs.extend(str(c) for c in self.constraints)
        definition = ",\n".join(defs)
        return f"CREATE TABLE {self.name} (\n{definition}\n);"

    def create_table_stmt(self) -> str:
        defs: list[str] = []
        defs.extend(str(c) for c in self.columns)
        defs.append(f"PRIMARY KEY ({self.primary_key})")
        definition = ",\n".join(defs)
        return f"CREATE TABLE {self.name} (\n{definition}\n);"

    def alter_table_stmt(self) -> Optional[str]:
        if self.constraints:
            return (
                f"ALTER TABLE {self.name}\n"
                + ",\n".join(f"ADD {c}" for c in self.constraints)
                + "\n;"
            )
        else:
            return None


@dataclass
class Namespace:
    name: LocalId
    enums: list[EnumType]
    structs: list[StructType]
    tables: list[Table]

    def __str__(self) -> str:
        items: list[str] = [f"CREATE SCHEMA IF NOT EXISTS {self.name};"]
        items.extend(str(e) for e in self.enums)
        items.extend(str(s) for s in self.structs)
        items.extend(t.create_table_stmt() for t in self.tables)
        items.extend(filter(None, (t.alter_table_stmt() for t in self.tables)))
        return "\n".join(items)
