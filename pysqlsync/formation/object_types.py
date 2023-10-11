import abc
import enum
import typing
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Optional, overload

from ..model.data_types import SqlDataType, constant, quote
from ..model.id_types import LocalId, QualifiedId, SupportsQualifiedId
from .object_dict import ObjectDict


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

    def __init__(self, enum_id: QualifiedId, values: list[str]) -> None:
        super().__init__(enum_id)
        self.values = values

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
                f"operation not permitted; cannot drop values in an enumeration: {''.join(source_values - target_values)}"
            )

        diff_values = list(target_values - source_values)
        diff_values.sort()
        return (
            (
                f"ALTER TYPE {source.name}\n"
                + ",\n".join(f"ADD VALUE {quote(v)}" for v in diff_values)
                + ";"
            )
            if diff_values
            else None
        )

    def __str__(self) -> str:
        return self.create_stmt()


@dataclass
class StructMember:
    "A member of a struct type."

    name: LocalId
    data_type: SqlDataType
    description: Optional[str] = None

    def __str__(self) -> str:
        return f"{self.name} {self.data_type}"


@dataclass
class StructType(QualifiedObject, MutableObject):
    "A struct type, i.e. a nested type without a primary key."

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


@dataclass(eq=True)
class Column(MutableObject):
    """
    A column in a database table.

    :param name: The name of the column within its host table.
    :param data_type: The SQL data type of the column.
    :param nullable: True if the column can be NULL.
    :param default: The default value the column takes if no explicit value is set.
    :param identity: Whether the column is an identity column.
    :param description: The textual description of the column.
    """

    name: LocalId
    data_type: SqlDataType
    nullable: bool
    default: Optional[Any] = None
    identity: bool = False
    description: Optional[str] = None

    def __str__(self) -> str:
        return self.column_spec

    @property
    def column_spec(self) -> str:
        return f"{self.name} {self.data_spec}"

    @property
    def data_spec(self) -> str:
        nullable = " NOT NULL" if not self.nullable else ""
        default = (
            f" DEFAULT {constant(self.default)}" if self.default is not None else ""
        )
        identity = " GENERATED BY DEFAULT AS IDENTITY" if self.identity else ""
        return f"{self.data_type}{nullable}{default}{identity}"

    def create_stmt(self) -> str:
        return f"ADD COLUMN {self.column_spec}"

    def drop_stmt(self) -> str:
        return f"DROP COLUMN {self.name}"

    def mutate_stmt(self, src: MutableObject) -> Optional[str]:
        source = typing.cast(Column, src)
        target = self

        if source == target:
            return None

        statements = target.mutate_column_stmt(source)
        if statements:
            return ",\n".join(statements)
        else:
            return None

    def mutate_column_stmt(target: "Column", source: "Column") -> list[str]:
        statements: list[str] = []

        if source.data_type != target.data_type:
            statements.append(f"SET DATA TYPE {target.data_type}")

        if source.nullable and not target.nullable:
            statements.append("SET NOT NULL")
        elif not source.nullable and target.nullable:
            statements.append("DROP NOT NULL")

        if source.default is not None and target.default is None:
            statements.append("DROP DEFAULT")
        elif source.default != target.default:
            statements.append(f"SET DEFAULT {constant(target.default)}")

        if source.identity and not target.identity:
            statements.append("DROP IDENTITY")
        elif not source.identity and target.identity:
            statements.append("ADD GENERATED BY DEFAULT AS IDENTITY")

        return [f"ALTER COLUMN {source.name} {s}" for s in statements]


@dataclass
class ConstraintReference:
    """
    A reference that a constraint points to.

    :param table: The table that the constraint points to.
    :param column: The column in the table that the constraint points to.
    """

    table: SupportsQualifiedId
    column: LocalId


@dataclass
class Constraint:
    """
    A table constraint, such as a primary, foreign or check constraint.

    :param name: The name of the constraint.
    """

    name: LocalId

    def is_alter_table(self) -> bool:
        "True if the constraint is to be applied with an ALTER TABLE statement."

        return False


@dataclass
class UniqueConstraint(Constraint):
    "A unique constraint."

    unique_column: LocalId

    def is_alter_table(self) -> bool:
        return True

    def __str__(self) -> str:
        return f"CONSTRAINT {self.name} UNIQUE ({self.unique_column})"


@dataclass
class ReferenceConstraint(Constraint):
    "A constraint that references another table, such as a foreign or discriminated key constraint."

    foreign_column: LocalId


@dataclass
class ForeignConstraint(ReferenceConstraint):
    "A foreign key constraint."

    reference: ConstraintReference

    def is_alter_table(self) -> bool:
        return True

    def __str__(self) -> str:
        return f"CONSTRAINT {self.name} FOREIGN KEY ({self.foreign_column}) REFERENCES {self.reference.table} ({self.reference.column})"


@dataclass
class DiscriminatedConstraint(ReferenceConstraint):
    """
    A discriminated key constraint whose value references one of several tables.

    :param references: The list of tables either of which the constraint can point to.
    """

    references: list[ConstraintReference]


@dataclass
class CheckConstraint(Constraint):
    "A check constraint."

    condition: str

    def is_alter_table(self) -> bool:
        return True

    def __str__(self) -> str:
        return f"CONSTRAINT {self.name} CHECK ({self.condition})"


@dataclass
class Table(QualifiedObject, MutableObject):
    """
    A database table.

    :param columns: The columns that the table consists of.
    :param primary_key: The primary key of the table.
    :param constraints: Any constraints applied to the table.
    :param description: A textual description of the table.
    """

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

    def is_primary_column(self, column_id: LocalId) -> bool:
        "True if the specified column is a primary key."

        return column_id == self.primary_key

    def get_primary_column(self) -> Column:
        "Returns the primary key column."

        for column in self.columns.values():
            if column.name == self.primary_key:
                return column

        raise KeyError(f"no primary column in table: {self.name}")

    def get_value_columns(self) -> list[Column]:
        "Returns all columns that are not part of the primary key."

        return [
            column
            for column in self.columns.values()
            if column.name != self.primary_key
        ]

    def is_unique_column(self, column_id: LocalId) -> bool:
        "True if a unique constraint is applied to the specified column."

        if self.constraints is not None:
            for constraint in self.constraints:
                if not isinstance(constraint, UniqueConstraint):
                    continue
                if column_id != constraint.unique_column:
                    continue
                return True

        return False

    def is_lookup_table(self) -> bool:
        "Checks whether the table maps a primary key to a unique value."

        if len(self.columns) != 2:
            return False

        for column in self.columns.values():
            if self.is_primary_column(column.name):
                continue
            if self.is_unique_column(column.name):
                continue
            return False

        return True

    def is_relation(self, column_id: LocalId) -> bool:
        "Checks whether the column is a foreign key relation."

        if self.constraints is not None:
            for constraint in self.constraints:
                if not isinstance(constraint, ForeignConstraint):
                    continue
                if column_id != constraint.foreign_column:
                    continue
                return True

        return False

    def get_reference(self, column_id: LocalId) -> ConstraintReference:
        "Returns a reference that a column points to."

        if self.constraints is not None:
            for constraint in self.constraints:
                if not isinstance(constraint, ForeignConstraint):
                    continue
                if column_id != constraint.foreign_column:
                    continue
                return constraint.reference

        raise KeyError(f"foreign constraint not found for column: {column_id}")

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
            if source_column.name.id not in target.columns:
                statements.append(source_column.drop_stmt())
        for target_column in target.columns.values():
            source_column = source.columns.get(target_column.name.id)
            if source_column is None:
                statements.append(target_column.create_stmt())
            else:
                statement = target_column.mutate_stmt(source_column)
                if statement:
                    statements.append(statement)

        if source.constraints and not target.constraints:
            for constraint in source.constraints:
                if constraint.is_alter_table():
                    statements.append(f"DROP CONSTRAINT {constraint.name}")
        elif not source.constraints and target.constraints:
            for constraint in target.constraints:
                if constraint.is_alter_table():
                    statements.append(f"ADD {str(constraint)}")
        elif source.constraints and target.constraints:
            for target_constraint in target.constraints:
                ...

        if statements:
            return f"ALTER TABLE {source.name}\n" + ",\n".join(statements) + ";"
        else:
            return None

    def add_constraints_stmt(self) -> Optional[str]:
        if self.constraints and any(c.is_alter_table() for c in self.constraints):
            return (
                f"ALTER TABLE {self.name}\n"
                + ",\n".join(f"ADD {c}" for c in self.constraints if c.is_alter_table())
                + "\n;"
            )
        else:
            return None

    def drop_constraints_stmt(self) -> Optional[str]:
        if self.constraints and any(c.is_alter_table() for c in self.constraints):
            return (
                f"ALTER TABLE {self.name}\n"
                + ",\n".join(
                    f"DROP CONSTRAINT {c.name}"
                    for c in self.constraints
                    if c.is_alter_table()
                )
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
    "A namespace that multiple objects can share. Typically corresponds to a database schema."

    name: LocalId
    enums: ObjectDict[EnumType]
    structs: ObjectDict[StructType]
    tables: ObjectDict[Table]

    @overload
    def __init__(self) -> None:
        ...

    @overload
    def __init__(
        self,
        name: LocalId,
        *,
        enums: list[EnumType],
        structs: list[StructType],
        tables: list[Table],
    ) -> None:
        ...

    def __init__(
        self,
        name: Optional[LocalId] = None,
        *,
        enums: Optional[list[EnumType]] = None,
        structs: Optional[list[StructType]] = None,
        tables: Optional[list[Table]] = None,
    ) -> None:
        self.name = name or LocalId("")
        self.enums = ObjectDict(enums or [])
        self.structs = ObjectDict(structs or [])
        self.tables = ObjectDict(tables or [])

    def create_stmt(self) -> str:
        items: list[str] = []
        if self.name.local_id:
            items.append(f"CREATE SCHEMA IF NOT EXISTS {self.name};")
        items.extend(str(e) for e in self.enums.values())
        items.extend(str(s) for s in self.structs.values())
        items.extend(t.create_stmt() for t in self.tables.values())
        return "\n".join(items)

    def add_constraints_stmt(self) -> Optional[str]:
        items: list[str] = []
        for table in self.tables.values():
            constraints = table.add_constraints_stmt()
            if constraints is None:
                continue
            items.append(constraints)

        return "\n".join(items) if items else None

    def drop_constraints_stmt(self) -> Optional[str]:
        items: list[str] = []
        for table in self.tables.values():
            constraints = table.drop_constraints_stmt()
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

        for id in target.tables.keys():
            if id not in source.tables.keys():
                statement = target.tables[id].add_constraints_stmt()
                if statement:
                    statements.append(statement)

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
    "A collection of database objects. Typically corresponds to a complete database."

    namespaces: ObjectDict[Namespace]

    def __init__(
        self,
        namespaces: list[Namespace],
    ) -> None:
        self.namespaces = ObjectDict(namespaces)

    def get_table(self, table_id: SupportsQualifiedId) -> Table:
        """
        Looks up a table by its qualified name.

        :param table_id: Identifies the table in the catalog.
        :returns: The table identified by the qualified name.
        """

        return self.namespaces[table_id.scope_id or ""].tables[table_id.local_id]

    def get_referenced_table(
        self, table_id: SupportsQualifiedId, column_id: LocalId
    ) -> Table:
        """
        Looks up a table referenced by a foreign key column.

        :param table_id: Identifies the table in the catalog.
        :param column_id: Identifies the foreign key column.
        :returns: The table in which the referenced primary key is.
        """

        table = self.get_table(table_id)
        reference = table.get_reference(column_id)
        return self.get_table(reference.table)

    def create_stmt(self) -> str:
        return "\n".join(n.create_stmt() for n in self.namespaces.values())

    def add_constraints_stmt(self) -> Optional[str]:
        items: list[str] = []
        for namespace in self.namespaces.values():
            constraints = namespace.add_constraints_stmt()
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
        for id in target.namespaces.keys():
            if id not in source.namespaces.keys():
                statement = target.namespaces[id].add_constraints_stmt()
                if statement:
                    statements.append(statement)

        statements.extend(_mutate_diff(source.namespaces, target.namespaces))

        for id in source.namespaces.keys():
            if id not in target.namespaces.keys():
                statement = source.namespaces[id].drop_constraints_stmt()
                if statement:
                    statements.append(statement)
        statements.extend(_drop_diff(source.namespaces, target.namespaces))
        return "\n".join(statements)

    def __str__(self) -> str:
        statements: list[str] = []
        statements.append(self.create_stmt())
        constraints = self.add_constraints_stmt()
        if constraints is not None:
            statements.append(constraints)
        return "\n".join(statements)
