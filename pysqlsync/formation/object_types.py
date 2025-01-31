import abc
import copy
from dataclasses import dataclass
from typing import Any, Iterable, Optional, overload

from strong_typing.inspection import is_dataclass_instance
from strong_typing.topological import topological_sort

from ..model.data_types import SqlDataType, SqlUserDefinedType, constant
from ..model.id_types import LocalId, SupportsQualifiedId
from .object_dict import ObjectDict


def deleted(name: str) -> str:
    "Name for a soft-deleted object."

    return f"pysqlsync${name}"


class StatementList(list[str]):
    def append(self, __object: Optional[str]) -> None:
        if __object is None:
            return
        if isinstance(__object, str) and not __object.strip():
            raise ValueError("empty statement")
        return super().append(__object)

    def extend(self, __iterable: Iterable[Optional[str]]) -> None:
        for item in __iterable:
            self.append(item)


def join(statements: Iterable[Optional[str]]) -> str:
    return "\n".join(s for s in statements if s is not None)


def join_or_none(statements: Iterable[Optional[str]]) -> Optional[str]:
    return join(statements) or None


class MappingError(RuntimeError):
    "Raised when a Python class cannot map to a database entity."


class FormationError(RuntimeError):
    "Raised when a source state cannot mutate into a target state."


class ColumnFormationError(FormationError):
    "Raised when a column cannot mutate into another column."

    column: LocalId

    def __init__(self, cause: str, column: LocalId) -> None:
        super().__init__(cause)
        self.column = column

    def __str__(self) -> str:
        return f"column {self.column}: {self.args[0]}"


class TableFormationError(FormationError):
    """
    Raised when a table cannot mutate into another table.

    Details on why the mutation failed are available in the stack trace.
    """

    table: SupportsQualifiedId

    def __init__(self, cause: str, table: SupportsQualifiedId) -> None:
        super().__init__(cause)
        self.table = table

    def __str__(self) -> str:
        return f"table {self.table}: {self.args[0]}"


@dataclass
class QualifiedObject(abc.ABC):
    name: SupportsQualifiedId


class DatabaseObject(abc.ABC):
    @abc.abstractmethod
    def create_stmt(self) -> str: ...

    @abc.abstractmethod
    def drop_stmt(self) -> str: ...


@dataclass
class EnumType(DatabaseObject, QualifiedObject):
    values: list[str]

    def __init__(self, enum_id: SupportsQualifiedId, values: list[str]) -> None:
        super().__init__(enum_id)
        self.values = values

    def create_stmt(self) -> str:
        vals = ", ".join(constant(val) for val in self.values)
        return f"CREATE TYPE {self.name} AS ENUM ({vals});"

    def drop_stmt(self) -> str:
        return f"DROP TYPE {self.name};"

    def soft_drop_stmt(self) -> str:
        "Causes the enumeration type to become inaccessible by obfuscating its name."

        return self.rename_stmt(deleted(self.name.local_id))

    def hard_drop_stmt(self) -> str:
        "Removes the enumeration type that had previously been soft-deleted."

        return f"DROP TYPE {self.name.rename(deleted(self.name.local_id))};"

    def rename_stmt(self, name: str) -> str:
        return f"ALTER TYPE {self.name} RENAME TO {LocalId(name)};"

    def __str__(self) -> str:
        return self.create_stmt()


@dataclass
class StructMember:
    """
    A member (a.k.a. field) of a composite (a.k.a. struct) type.

    :param name: Name of the field.
    :param data_type: Type of the field.
    :param description: Human-readable description of the field.
    """

    name: LocalId
    data_type: SqlDataType
    description: Optional[str] = None

    def __str__(self) -> str:
        return f"{self.name} {self.data_type}"


@dataclass
class StructType(DatabaseObject, QualifiedObject):
    """
    A composite (a.k.a. struct) type, i.e. a nested type without a primary key.

    :param members: Members (a.k.a. fields) of the composite type.
    :param description: Human-readable description of the composite type.
    """

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

    def get_referenced_types(self) -> set[SupportsQualifiedId]:
        return set(
            m.data_type.ref
            for m in self.members.values()
            if isinstance(m.data_type, SqlUserDefinedType)
        )

    def create_stmt(self) -> str:
        members = ",\n".join(str(m) for m in self.members.values())
        return f"CREATE TYPE {self.name} AS (\n{members}\n);"

    def drop_stmt(self) -> str:
        return f"DROP TYPE {self.name};"

    def __str__(self) -> str:
        return self.create_stmt()


@dataclass(eq=True)
class Column(DatabaseObject):
    """
    A column in a database table.

    :param name: The name of the column within its host table.
    :param data_type: The SQL data type of the column.
    :param nullable: True if the column can take the value NULL.
    :param default: The default value the column takes if no explicit value is set. Must be a valid SQL expression.
    :param identity: Whether the column is an identity column.
    :param description: The textual description of the column.
    """

    name: LocalId
    data_type: SqlDataType
    nullable: bool
    default: Optional[str] = None
    identity: bool = False
    description: Optional[str] = None

    def __str__(self) -> str:
        return self.column_spec

    @property
    def column_spec(self) -> str:
        return f"{self.name} {self.data_spec}"

    @property
    def data_spec(self) -> str:
        nullable = " NOT NULL" if not self.nullable and not self.identity else ""
        default = f" DEFAULT {self.default}" if self.default is not None else ""
        identity = " GENERATED BY DEFAULT AS IDENTITY" if self.identity else ""
        return f"{self.data_type}{nullable}{default}{identity}"

    def create_stmt(self) -> str:
        "Creates a column as part of an ALTER TABLE statement."

        return f"ADD COLUMN {self.column_spec}"

    def drop_stmt(self) -> str:
        "Removes a column as part of an ALTER TABLE statement."

        return f"DROP COLUMN {self.name}"

    def soft_drop_stmt(self) -> str:
        "Causes the column to become inaccessible by obfuscating its name."

        return self.rename_stmt(deleted(self.name.local_id))

    def hard_drop_stmt(self) -> str:
        "Removes the column that had previously been soft-deleted."

        return f"DROP COLUMN {LocalId(deleted(self.name.local_id))}"

    def rename_stmt(self, name: str) -> str:
        return f"RENAME COLUMN {self.name} TO {LocalId(name)}"


@dataclass
class ConstraintReference:
    """
    A reference that a constraint points to.

    :param table: The table that the constraint points to.
    :param columns: The columns in the table that the constraint points to.
    """

    table: SupportsQualifiedId
    columns: tuple[LocalId, ...]


@dataclass
class Constraint(abc.ABC):
    """
    A table constraint, such as a primary, foreign or check constraint.

    :param name: The name of the constraint.
    """

    name: LocalId

    @property
    @abc.abstractmethod
    def spec(self) -> str: ...

    def is_alter_table(self) -> bool:
        "True if the constraint is to be applied with an ALTER TABLE statement."

        return False

    def __str__(self) -> str:
        return f"CONSTRAINT {self.spec}"


@dataclass
class UniqueConstraint(Constraint):
    """
    A unique constraint.

    :param unique_columns: Names of columns that comprise the constraint.
    """

    unique_columns: tuple[LocalId, ...]

    def is_alter_table(self) -> bool:
        return True

    @property
    def spec(self) -> str:
        columns = ", ".join(str(column) for column in self.unique_columns)
        return f"{self.name} UNIQUE ({columns})"


@dataclass
class ReferenceConstraint(Constraint):
    """
    A constraint that references another table, such as a foreign or discriminated key constraint.

    :param foreign_columns: Names of columns that comprise the reference constraint.
    """

    foreign_columns: tuple[LocalId, ...]


@dataclass
class ForeignConstraint(ReferenceConstraint):
    """
    A foreign key constraint.

    :param reference: The reference this foreign constraint incorporates.
    """

    reference: ConstraintReference

    def is_alter_table(self) -> bool:
        return True

    @property
    def spec(self) -> str:
        source_columns = ", ".join(str(column) for column in self.foreign_columns)
        target_columns = ", ".join(str(column) for column in self.reference.columns)
        return f"{self.name} FOREIGN KEY ({source_columns}) REFERENCES {self.reference.table} ({target_columns})"


@dataclass
class DiscriminatedConstraint(ReferenceConstraint):
    """
    A discriminated key constraint whose value references one of several tables.

    :param references: The list of tables either of which the constraint can point to.
    """

    references: list[ConstraintReference]

    @property
    def spec(self) -> str:
        raise NotImplementedError(
            "cannot represent discriminated constraint in SQL DDL"
        )


@dataclass
class CheckConstraint(Constraint):
    """
    A check constraint.

    :param condition: A Boolean expression in SQL that must hold true for the column values.
    """

    condition: str

    def is_alter_table(self) -> bool:
        return True

    @property
    def spec(self) -> str:
        return f"{self.name} CHECK ({self.condition})"


@dataclass
class Table(DatabaseObject, QualifiedObject):
    """
    A database table.

    :param columns: The columns that the table consists of.
    :param primary_key: The primary key column(s) of the table.
    :param constraints: Any constraints applied to the table.
    :param description: A textual description of the table.
    """

    columns: ObjectDict[Column]
    primary_key: tuple[LocalId, ...]
    constraints: ObjectDict[Constraint]
    description: Optional[str]

    def __init__(
        self,
        name: SupportsQualifiedId,
        columns: list[Column],
        *,
        primary_key: tuple[LocalId, ...],
        constraints: Optional[list[Constraint]] = None,
        description: Optional[str] = None,
    ) -> None:
        super().__init__(name)
        self.columns = ObjectDict(columns)
        self.primary_key = primary_key
        self.constraints = ObjectDict(constraints or [])
        self.description = description

    def __str__(self) -> str:
        defs: list[str] = []
        defs.extend(str(c) for c in self.columns.values())
        keys = ", ".join(str(key) for key in self.primary_key)
        defs.append(f"CONSTRAINT {self.primary_key_constraint_id} PRIMARY KEY ({keys})")
        defs.extend(str(c) for c in self.constraints.values())
        definition = ",\n".join(defs)
        return f"CREATE TABLE {self.name} (\n{definition}\n);"

    @property
    def primary_key_constraint_id(self) -> LocalId:
        return LocalId(f"pk_{self.name.compact_id.replace('.', '_')}")

    def get_columns(
        self, field_names: Optional[tuple[str, ...]] = None
    ) -> list[Column]:
        "Returns columns of a table in a desired order."

        if field_names is not None:
            columns: list[Column] = []

            for field_name in field_names:
                column = self.columns.get(field_name)
                if column is None:
                    raise ValueError(
                        f"column {LocalId(field_name)} not found in table {self.name}"
                    )
                columns.append(column)

            return columns
        else:
            return list(self.columns.values())

    def check_primary(self) -> None:
        if len(self.primary_key) == 0:
            raise KeyError(f"no primary key defined in table: {self.name}")
        if len(self.primary_key) > 1:
            raise KeyError(f"composite primary defined in table: {self.name}")

    def is_primary_column(self, column_id: LocalId) -> bool:
        "True if the specified column is a primary key."

        self.check_primary()
        return column_id in self.primary_key

    def get_primary_column(self) -> Column:
        "Returns the primary key column."

        self.check_primary()
        for column in self.columns.values():
            if column.name in self.primary_key:
                return column

        raise KeyError(f"no primary column in table: {self.name}")

    def get_value_columns(
        self, field_names: Optional[tuple[str, ...]] = None
    ) -> list[Column]:
        """
        Returns columns that have to be supplied an explicit value for a newly inserted row.

        This constitutes all columns that not part of the primary key and not generated by identity.
        """

        return [
            column
            for column in self.get_columns(field_names)
            if column.name not in self.primary_key and not column.identity
        ]

    def is_unique_column(self, column: Column) -> bool:
        "True if a unique constraint is applied to the specified column."

        for constraint in self.constraints.values():
            if not isinstance(constraint, UniqueConstraint):
                continue
            if len(constraint.unique_columns) > 1:
                continue
            if column.name not in constraint.unique_columns:
                continue
            return True

        return False

    def is_lookup_column(self, column: Column) -> bool:
        "True if the column may be used to look up a record by its value."

        return self.is_primary_column(column.name) or self.is_unique_column(column)

    def is_lookup_table(self) -> bool:
        "Checks whether the table maps a primary key to a unique value."

        if len(self.columns) != 2:
            return False

        for column in self.columns.values():
            if self.is_primary_column(column.name):
                continue
            if self.is_unique_column(column):
                continue
            return False

        return True

    def is_relation(self, column: Column) -> bool:
        "Checks whether the column is a foreign key relation."

        for constraint in self.constraints.values():
            if not isinstance(constraint, ForeignConstraint):
                continue
            if len(constraint.foreign_columns) > 1:
                continue
            if column.name not in constraint.foreign_columns:
                continue
            return True

        return False

    def get_constraint(self, column_id: LocalId) -> ConstraintReference:
        "Returns a reference that a column points to."

        for constraint in self.constraints.values():
            if not isinstance(constraint, ForeignConstraint):
                continue
            if len(constraint.foreign_columns) > 1:
                continue
            if column_id not in constraint.foreign_columns:
                continue
            return constraint.reference

        raise KeyError(f"foreign constraint not found for column: {column_id}")

    def get_referenced_types(self) -> set[SupportsQualifiedId]:
        return set(
            c.data_type.ref
            for c in self.columns.values()
            if isinstance(c.data_type, SqlUserDefinedType)
        )

    def get_referenced_tables(self) -> set[SupportsQualifiedId]:
        items: set[SupportsQualifiedId] = set()
        for c in self.constraints.values():
            if isinstance(c, ForeignConstraint):
                items.add(c.reference.table)
            elif isinstance(c, DiscriminatedConstraint):
                items.update(r.table for r in c.references)
        return items

    def create_keys(self) -> str:
        keys = ", ".join(str(key) for key in self.primary_key)
        return f"CONSTRAINT {self.primary_key_constraint_id} PRIMARY KEY ({keys})"

    def create_stmt(self) -> str:
        defs: list[str] = []
        defs.extend(str(c) for c in self.columns.values())
        defs.append(self.create_keys())
        definition = ",\n".join(defs)
        return f"CREATE TABLE {self.name} (\n{definition}\n);"

    def drop_stmt(self) -> str:
        return f"DROP TABLE {self.name};"

    def drop_if_exists_stmt(self) -> str:
        return f"DROP TABLE IF EXISTS {self.name};"

    def alter_table_stmt(self, statements: list[str]) -> str:
        return f"ALTER TABLE {self.name}\n" + ",\n".join(statements) + ";"

    def add_constraints_stmt(self) -> Optional[str]:
        if self.table_constraints:
            return self.alter_table_stmt(
                [f"ADD CONSTRAINT {c.spec}" for c in self.table_constraints]
            )
        else:
            return None

    def drop_constraints_stmt(self) -> Optional[str]:
        if self.table_constraints:
            return (
                f"ALTER TABLE {self.name}\n"
                + ",\n".join(
                    f"DROP CONSTRAINT {c.name}" for c in self.table_constraints
                )
                + "\n;"
            )
        else:
            return None

    @property
    def table_constraints(self) -> list[Constraint]:
        return [c for c in self.constraints.values() if c.is_alter_table()]


class EnumTable(Table):
    values: list[Any]

    def __init__(
        self,
        name: SupportsQualifiedId,
        columns: list[Column],
        *,
        values: list[Any],
        primary_key: tuple[LocalId, ...],
        constraints: Optional[list[Constraint]] = None,
        description: Optional[str] = None,
    ) -> None:
        super().__init__(
            name,
            columns,
            primary_key=primary_key,
            constraints=constraints,
            description=description,
        )
        self.values = values

    def create_stmt(self) -> str:
        statements: list[str] = []
        statements.append(super().create_stmt())
        column_list = ", ".join(str(col.name) for col in self.get_value_columns())
        values: list[str] = []
        for value in self.values:
            if isinstance(value, tuple) or is_dataclass_instance(value):
                values.append(constant(value))
            else:
                values.append(f"({constant(value)})")
        value_list = ", ".join(values)

        statements.append(
            f"INSERT INTO {self.name} ({column_list}) VALUES {value_list};"
        )
        return "\n".join(statements)


@dataclass
class Namespace(DatabaseObject):
    """
    A namespace that multiple objects can share. Typically corresponds to a database schema.

    :param name: Unqualified name for the namespace (schema).
    :param enums: Enumeration types defined in the namespace (if the database engine supports them).
    :param structs: Composite types defined in the namespace (if the database engine supports them).
    :param tables: Tables defined in the namespace.
    """

    name: LocalId
    enums: ObjectDict[EnumType]
    structs: ObjectDict[StructType]
    tables: ObjectDict[Table]

    @overload
    def __init__(self) -> None: ...

    @overload
    def __init__(self, name: LocalId) -> None: ...

    @overload
    def __init__(
        self,
        *,
        enums: list[EnumType],
        structs: list[StructType],
        tables: list[Table],
    ) -> None: ...

    @overload
    def __init__(
        self,
        name: LocalId,
        *,
        enums: list[EnumType],
        structs: list[StructType],
        tables: list[Table],
    ) -> None: ...

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

    def __bool__(self) -> bool:
        return len(self.enums) > 0 or len(self.structs) > 0 or len(self.tables) > 0

    def get_referenced_namespaces(self) -> set[LocalId]:
        items: set[SupportsQualifiedId] = set()
        for s in self.structs.values():
            items.update(s.get_referenced_types())
        for t in self.tables.values():
            items.update(t.get_referenced_types())
            items.update(t.get_referenced_tables())
        return set(LocalId(i.scope_id) for i in items if i.scope_id is not None)

    def create_schema_stmt(self) -> Optional[str]:
        if self.name.local_id:
            return f"CREATE SCHEMA {self.name};"
        else:
            return None

    def drop_schema_stmt(self) -> Optional[str]:
        if self.name.local_id:
            return f"DROP SCHEMA {self.name};"
        else:
            return None

    def create_stmt(self) -> str:
        return join([self.create_schema_stmt(), self.create_objects_stmt()])

    def drop_stmt(self) -> str:
        return join([self.drop_objects_stmt(), self.drop_schema_stmt()])

    def create_objects_stmt(self) -> Optional[str]:
        items: list[str] = []
        items.extend(str(e) for e in self.enums.values())
        items.extend(str(s) for s in self.structs.values())
        items.extend(t.create_stmt() for t in self.tables.values())
        return join_or_none(items)

    def drop_objects_stmt(self) -> Optional[str]:
        items: list[str] = []
        items.extend(t.drop_stmt() for t in reversed(list(self.tables.values())))
        items.extend(s.drop_stmt() for s in reversed(list(self.structs.values())))
        items.extend(e.drop_stmt() for e in reversed(list(self.enums.values())))
        return join_or_none(items)

    def add_constraints_stmt(self) -> Optional[str]:
        return join_or_none(
            table.add_constraints_stmt() for table in self.tables.values()
        )

    def drop_constraints_stmt(self) -> Optional[str]:
        return join_or_none(
            table.drop_constraints_stmt() for table in self.tables.values()
        )

    def merge(self, op: "Namespace") -> None:
        "Merges the contents of two objects."

        for enum_name, op_enum in op.enums.items():
            self_enum = self.enums.get(enum_name)
            if self_enum is not None:
                if self_enum != op_enum:
                    raise FormationError(
                        f"different definitions for {self_enum.name} and {op_enum.name}"
                    )
            else:
                self.enums.add(copy.deepcopy(op_enum))

        for struct_name, op_struct in op.structs.items():
            self_struct = self.structs.get(struct_name)
            if self_struct is not None:
                if self_struct != op_struct:
                    raise FormationError(
                        f"different definitions for {self_struct.name} and {op_struct.name}"
                    )
            else:
                self.structs.add(copy.deepcopy(op_struct))

        for table_name, op_table in op.tables.items():
            self_table = self.tables.get(table_name)
            if self_table is not None:
                if self_table != op_table:
                    raise FormationError(
                        f"different definitions for {self_table.name} and {op_table.name}"
                    )
            else:
                self.tables.add(copy.deepcopy(op_table))

    def __str__(self) -> str:
        return self.create_stmt()


@dataclass
class Catalog(DatabaseObject):
    """
    A collection of database objects. Typically corresponds to a complete database.

    For databases without namespace (schema) support, the only member in the collection is the empty string (`""`).

    :param namespaces: Collection of namespaces (schemas) defined in the database.
    """

    namespaces: ObjectDict[Namespace]

    def __init__(
        self,
        namespaces: Optional[list[Namespace]] = None,
    ) -> None:
        self.namespaces = ObjectDict(namespaces or [])

    def __bool__(self) -> bool:
        return len(self.namespaces) > 0

    def get_table(self, table_id: SupportsQualifiedId) -> Table:
        """
        Looks up a table by its qualified name.

        :param table_id: Identifies the table in the catalog.
        :returns: The table identified by the qualified name.
        """

        if not self.namespaces:
            raise MappingError("empty namespace")

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
        reference = table.get_constraint(column_id)
        return self.get_table(reference.table)

    def create_stmt(self) -> str:
        self.sort()
        items: list[Optional[str]] = []
        items.extend(n.create_schema_stmt() for n in self.namespaces.values())
        items.extend(n.create_objects_stmt() for n in self.namespaces.values())
        return join(items)

    def add_constraints_stmt(self) -> Optional[str]:
        return join_or_none(n.add_constraints_stmt() for n in self.namespaces.values())

    def drop_stmt(self) -> str:
        self.sort()
        items: list[Optional[str]] = []
        items.extend(
            n.drop_objects_stmt() for n in reversed(list(self.namespaces.values()))
        )
        items.extend(
            n.drop_schema_stmt() for n in reversed(list(self.namespaces.values()))
        )
        return join(items)

    def sort(self) -> None:
        """
        Sort namespaces in topological order of reference.

        Namespaces that have no external references come first. The next group contains namespaces that reference
        items only in the first group of namespaces, etc.
        """

        graph = {
            ns.name: ns.get_referenced_namespaces() for ns in self.namespaces.values()
        }
        for ref_ns in graph.values():
            ref_ns.intersection_update(graph.keys())
        order = [name.local_id for name in topological_sort(graph)]
        self.namespaces.reorder(order)

    def merge(self, op: "Catalog") -> None:
        "Merges the contents of two objects."

        for name, op_ns in op.namespaces.items():
            if name in self.namespaces:
                self.namespaces[name].merge(op_ns)
            else:
                self.namespaces.add(copy.deepcopy(op_ns))

    def __str__(self) -> str:
        return join([self.create_stmt(), self.add_constraints_stmt()])


class ObjectFactory:
    "Creates new column, table, struct and namespace instances."

    @property
    def column_class(self) -> type[Column]:
        "The object type instantiated for table columns."

        return Column

    @property
    def table_class(self) -> type[Table]:
        "The object type instantiated for tables."

        return Table

    @property
    def enum_table_class(self) -> type[EnumTable]:
        "The object type instantiated for tables that store enumeration values."

        return EnumTable

    @property
    def struct_class(self) -> type[StructType]:
        "The object type instantiated for struct types."

        return StructType

    @property
    def namespace_class(self) -> type[Namespace]:
        "The object type instantiated for namespaces (schemas)."

        return Namespace
