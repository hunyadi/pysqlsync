import abc
import enum
import typing
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Optional

from ..model.data_types import SqlDataType
from ..model.id_types import LocalId, QualifiedId, SupportsQualifiedId
from .object_dict import ObjectDict


def quote(s: str) -> str:
    "Quotes a string to be embedded in an SQL statement."

    return "'" + s.replace("'", "''") + "'"


class FormationError(RuntimeError):
    "Raised when a source state cannot mutate into a target state."


@dataclass
class QualifiedObject(abc.ABC):
    name: SupportsQualifiedId

    def check_identity(self, other: "QualifiedObject") -> None:
        if self.name != other.name:
            raise FormationError(f"object mismatch: {self.name} != {other.name}")


class MutableObject(abc.ABC):
    @abc.abstractmethod
    def create_stmt(self) -> str:
        ...

    @abc.abstractmethod
    def drop_stmt(self) -> str:
        ...

    @abc.abstractmethod
    def mutate_stmt(self, src: "MutableObject") -> Optional[str]:
        ...


@dataclass
class EnumType(QualifiedObject, MutableObject):
    values: list[str]

    def __init__(
        self, enum: type[enum.Enum], *, namespace: Optional[str] = None
    ) -> None:
        super().__init__(QualifiedId(namespace, enum.__name__))
        self.values = [str(e.value) for e in enum]

    def create_stmt(self) -> str:
        vals = ", ".join(quote(val) for val in self.values)
        return f"CREATE TYPE {self.name} AS ENUM ({vals});"

    def drop_stmt(self) -> str:
        return f"DROP TYPE {self.name};"

    def mutate_stmt(self, src: MutableObject) -> Optional[str]:
        source = typing.cast(EnumType, src)
        target = self
        self.check_identity(source)

        source_values = set(source.values)
        target_values = set(target.values)

        if source_values - target_values:
            raise FormationError(
                "operation not permitted; cannot drop values in an enumeration"
            )

        return (
            f"ALTER TYPE {source.name}\n"
            + ",\n".join(f"ADD VALUE {quote(v)}" for v in target_values - source_values)
            + ";"
        )

    def __str__(self) -> str:
        return self.create_stmt()


@dataclass
class StructMember:
    name: LocalId
    data_type: SqlDataType
    description: Optional[str] = None

    def __str__(self) -> str:
        return f"{self.name} {self.data_type}"


@dataclass
class StructType(QualifiedObject, MutableObject):
    members: ObjectDict[StructMember]
    description: Optional[str]

    def __init__(
        self,
        name: SupportsQualifiedId,
        members: list[StructMember],
        description: Optional[str] = None,
    ) -> None:
        super().__init__(name)
        self.members = ObjectDict(members)
        self.description = description

    def create_stmt(self) -> str:
        members = ",\n".join(str(m) for m in self.members.values())
        return f"CREATE TYPE {self.name} AS (\n{members}\n);"

    def drop_stmt(self) -> str:
        return f"DROP TYPE {self.name};"

    def mutate_stmt(self, src: MutableObject) -> Optional[str]:
        source = typing.cast(StructType, src)
        target = self
        self.check_identity(source)

        statements: list[str] = []
        for source_member in source.members.values():
            if source_member not in target.members.values():
                statements.append(f"DROP ATTRIBUTE {source_member.name}")
        for target_member in target.members.values():
            if target_member not in target.members.values():
                statements.append(f"ADD ATTRIBUTE {target_member}")
        if statements:
            return f"ALTER TYPE {source.name}\n" + ",\n".join(statements) + ";\n"
        else:
            return None

    def __str__(self) -> str:
        return self.create_stmt()


@dataclass
class Column(MutableObject):
    name: LocalId
    data_type: SqlDataType
    nullable: bool
    default: Optional[Any] = None
    description: Optional[str] = None

    def __str__(self) -> str:
        return self.column_spec

    @property
    def column_spec(self) -> str:
        return f"{self.name} {self.data_spec}"

    @property
    def data_spec(self) -> str:
        if self.nullable:
            return f"{self.data_type}"
        else:
            return f"{self.data_type} NOT NULL"

    def create_stmt(self) -> str:
        return f"ADD COLUMN {self.column_spec}"

    def drop_stmt(self) -> str:
        return f"DROP COLUMN {self.name}"

    def mutate_stmt(self, src: MutableObject) -> str | None:
        source = typing.cast(Column, src)
        target = self

        if source.nullable == target.nullable and source.data_type == target.data_type:
            return None

        if source.nullable and not target.nullable:
            raise FormationError("cannot make a nullable column non-nullable")

        return f"ALTER COLUMN {source.name} {target.data_spec}"


@dataclass
class ConstraintReference:
    table: SupportsQualifiedId
    column: LocalId


@dataclass
class Constraint:
    name: LocalId

    def is_alter_table(self) -> bool:
        return False


@dataclass
class ReferenceConstraint(Constraint):
    foreign_column: LocalId


@dataclass
class ForeignConstraint(ReferenceConstraint):
    reference: ConstraintReference

    def is_alter_table(self) -> bool:
        return True

    def __str__(self) -> str:
        return f"CONSTRAINT {self.name} FOREIGN KEY ({self.foreign_column}) REFERENCES {self.reference.table} ({self.reference.column})"


@dataclass
class DiscriminatedConstraint(ReferenceConstraint):
    references: list[ConstraintReference]


@dataclass
class CheckConstraint(Constraint):
    condition: str

    def is_alter_table(self) -> bool:
        return True

    def __str__(self) -> str:
        return f"CONSTRAINT {self.name} CHECK ({self.condition})"


@dataclass
class Table(QualifiedObject, MutableObject):
    columns: ObjectDict[Column]
    primary_key: LocalId
    constraints: Optional[list[Constraint]]
    description: Optional[str]

    def __init__(
        self,
        name: SupportsQualifiedId,
        columns: list[Column],
        *,
        primary_key: LocalId,
        constraints: Optional[list[Constraint]] = None,
        description: Optional[str] = None,
    ) -> None:
        super().__init__(name)
        self.columns = ObjectDict(columns)
        self.primary_key = primary_key
        self.constraints = constraints
        self.description = description

    def __str__(self) -> str:
        defs: list[str] = []
        defs.extend(str(c) for c in self.columns.values())
        defs.append(f"PRIMARY KEY ({self.primary_key})")
        if self.constraints is not None:
            defs.extend(str(c) for c in self.constraints)
        definition = ",\n".join(defs)
        return f"CREATE TABLE {self.name} (\n{definition}\n);"

    def create_stmt(self) -> str:
        defs: list[str] = []
        defs.extend(str(c) for c in self.columns.values())
        defs.append(f"PRIMARY KEY ({self.primary_key})")
        definition = ",\n".join(defs)
        return f"CREATE TABLE {self.name} (\n{definition}\n);"

    def drop_stmt(self) -> str:
        return f"DROP TABLE {self.name};"

    def mutate_stmt(self, src: MutableObject) -> Optional[str]:
        source = typing.cast(Table, src)
        target = self
        self.check_identity(source)

        statements: list[str] = []
        source_column: Optional[Column]
        for source_column in source.columns.values():
            if source_column not in target.columns.values():
                statements.append(source_column.drop_stmt())
        for target_column in target.columns.values():
            source_column = source.columns.get(target_column.name.id)
            if source_column is None:
                statements.append(target_column.create_stmt())
            else:
                statement = target_column.mutate_stmt(source_column)
                if statement:
                    statements.append(statement)
        if statements:
            return f"ALTER TABLE {source.name}\n" + ",\n".join(statements) + ";"
        else:
            return None

    def constraints_stmt(self) -> Optional[str]:
        if self.constraints and any(c.is_alter_table() for c in self.constraints):
            return (
                f"ALTER TABLE {self.name}\n"
                + ",\n".join(f"ADD {c}" for c in self.constraints if c.is_alter_table())
                + "\n;"
            )
        else:
            return None


def _create_diff(
    source: Mapping[str, MutableObject], target: Mapping[str, MutableObject]
) -> list[str]:
    return [target[id].create_stmt() for id in target.keys() if id not in source.keys()]


def _drop_diff(
    source: Mapping[str, MutableObject], target: Mapping[str, MutableObject]
) -> list[str]:
    return [source[id].drop_stmt() for id in source.keys() if id not in target.keys()]


def _mutate_diff(
    source: Mapping[str, MutableObject], target: Mapping[str, MutableObject]
) -> list[str]:
    statements: list[str] = []

    for id in source.keys():
        if id in target.keys():
            statement = target[id].mutate_stmt(source[id])
            if statement:
                statements.append(statement)

    return statements


@dataclass
class Namespace(MutableObject):
    name: LocalId
    enums: ObjectDict[EnumType]
    structs: ObjectDict[StructType]
    tables: ObjectDict[Table]

    def __init__(
        self,
        name: LocalId,
        enums: list[EnumType],
        structs: list[StructType],
        tables: list[Table],
    ) -> None:
        self.name = name
        self.enums = ObjectDict(enums)
        self.structs = ObjectDict(structs)
        self.tables = ObjectDict(tables)

    def create_stmt(self) -> str:
        items: list[str] = []
        if self.name.local_id:
            items.append(f"CREATE SCHEMA IF NOT EXISTS {self.name};")
        items.extend(str(e) for e in self.enums.values())
        items.extend(str(s) for s in self.structs.values())
        items.extend(t.create_stmt() for t in self.tables.values())
        return "\n".join(items)

    def constraints_stmt(self) -> Optional[str]:
        items: list[str] = []
        for table in self.tables.values():
            constraints = table.constraints_stmt()
            if constraints is None:
                continue
            items.append(constraints)

        return "\n".join(items) if items else None

    def drop_stmt(self) -> str:
        items: list[str] = []
        items.extend(t.drop_stmt() for t in reversed(self.tables.values()))
        items.extend(s.drop_stmt() for s in reversed(self.structs.values()))
        items.extend(e.drop_stmt() for e in reversed(self.enums.values()))
        if self.name.local_id:
            items.append(f"DROP SCHEMA {self.name};")
        return "\n".join(items)

    def mutate_stmt(self, src: MutableObject) -> Optional[str]:
        source = typing.cast(Namespace, src)
        target = self

        if source.name != target.name:
            raise FormationError(f"object mismatch: {source.name} != {target.name}")

        statements: list[str] = []

        statements.extend(_create_diff(source.enums, target.enums))
        statements.extend(_create_diff(source.structs, target.structs))
        statements.extend(_create_diff(source.tables, target.tables))

        statements.extend(_mutate_diff(source.enums, target.enums))
        statements.extend(_mutate_diff(source.structs, target.structs))
        statements.extend(_mutate_diff(source.tables, target.tables))

        statements.extend(_drop_diff(source.tables, target.tables))
        statements.extend(_drop_diff(source.structs, target.structs))
        statements.extend(_drop_diff(source.enums, target.enums))

        return "\n".join(statements)

    def __str__(self) -> str:
        return self.create_stmt()


@dataclass
class Catalog(MutableObject):
    namespaces: ObjectDict[Namespace]

    def __init__(
        self,
        namespaces: list[Namespace],
    ) -> None:
        self.namespaces = ObjectDict(namespaces)

    def create_stmt(self) -> str:
        return "\n".join(n.create_stmt() for n in self.namespaces.values())

    def constraints_stmt(self) -> Optional[str]:
        items: list[str] = []
        for namespace in self.namespaces.values():
            constraints = namespace.constraints_stmt()
            if constraints is None:
                continue
            items.append(constraints)

        return "\n".join(items) if items else None

    def drop_stmt(self) -> str:
        return "\n".join(n.drop_stmt() for n in self.namespaces.values())

    def mutate_stmt(self, src: MutableObject) -> Optional[str]:
        source = typing.cast(Catalog, src)
        target = self

        statements: list[str] = []
        statements.extend(_create_diff(source.namespaces, target.namespaces))
        statements.extend(_mutate_diff(source.namespaces, target.namespaces))
        statements.extend(_drop_diff(source.namespaces, target.namespaces))
        return "\n".join(statements)

    def __str__(self) -> str:
        statements: list[str] = []
        statements.append(self.create_stmt())
        constraints = self.constraints_stmt()
        if constraints is not None:
            statements.append(constraints)
        return "\n".join(statements)
