from dataclasses import dataclass
from typing import Optional

from ..model.data_types import (
    SqlEnumType,
    SqlIntegerType,
    SqlVariableCharacterType,
    constant,
    quote,
)
from ..model.id_types import SupportsName
from .object_types import (
    Catalog,
    Column,
    ColumnFormationError,
    EnumType,
    FormationError,
    Namespace,
    StatementList,
    StructType,
    Table,
    TableFormationError,
    join_or_none,
)


@dataclass
class MutatorOptions:
    """
    Options for synchronizing an existing source state with a desired target state.

    :param allow_drop_enum: Permit dropping enumeration types.
    :param allow_drop_struct: Permit dropping (non-table) structure types.
    :param allow_drop_table: Permit dropping tables.
    :param allow_drop_namespace: Permit dropping namespaces (database schemas) and all objects within.
    """

    allow_drop_enum: bool = True
    allow_drop_struct: bool = True
    allow_drop_table: bool = True
    allow_drop_namespace: bool = True


class Mutator:
    "Morphs an existing source state into a desired target state."

    options: MutatorOptions

    def __init__(self, options: Optional[MutatorOptions] = None) -> None:
        self.options = options or MutatorOptions()

    def check_identity(self, source: SupportsName, target: SupportsName) -> None:
        if source.name != target.name:
            raise FormationError(f"object mismatch: {source.name} != {target.name}")

    def migrate_enum_stmt(self, enum_type: EnumType, table: Table) -> Optional[str]:
        enum_values = ", ".join(f"({quote(v)})" for v in enum_type.values)
        return f'INSERT INTO {table.name} ("value") VALUES {enum_values};'

    def mutate_enum_stmt(self, source: EnumType, target: EnumType) -> Optional[str]:
        self.check_identity(source, target)

        removed_values = [
            value for value in source.values if value not in target.values
        ]
        if removed_values:
            raise FormationError(
                f"operation not permitted; cannot drop values in an enumeration: {''.join(removed_values)}"
            )

        added_values = [value for value in target.values if value not in source.values]
        if added_values:
            return "\n".join(
                f"ALTER TYPE {source.name} ADD VALUE {constant(v)};"
                for v in added_values
            )
        else:
            return None

    def mutate_struct_stmt(
        self, source: StructType, target: StructType
    ) -> Optional[str]:
        self.check_identity(source, target)

        statements: list[str] = []
        statements.extend(
            f"DROP ATTRIBUTE {member.name}"
            for member in source.members.difference(target.members)
        )
        for source_member, target_member in source.members.intersection(target.members):
            if source_member != target_member:
                statements.append(
                    f"ALTER ATTRIBUTE {source_member.name} SET DATA TYPE {target_member.data_type}"
                )
        statements.extend(
            f"ADD ATTRIBUTE {member}"
            for member in target.members.difference(source.members)
        )
        if statements:
            return f"ALTER TYPE {source.name}\n" + ",\n".join(statements) + ";\n"
        else:
            return None

    def migrate_column_stmt(
        self, source_table: Table, source: Column, target_table: Table, target: Column
    ) -> Optional[str]:
        return None

    def is_column_migrated(self, source: Column, target: Column) -> Optional[bool]:
        """
        True if the column requires data migration, false if no migration is needed.

        :param source: The source column to convert data from.
        :param target: The target column to convert data to.
        :returns: True/False, or None if subclass method is to decide.
        """

        # no migration is needed if column data type is unchanged
        if source == target or source.data_type == target.data_type:
            return False

        # check if target data type is a primary key type
        if isinstance(target.data_type, SqlIntegerType):
            if isinstance(source.data_type, SqlEnumType):
                return True
            # some database engines represent enumerations as `varchar`
            elif isinstance(source.data_type, SqlVariableCharacterType):
                return True

        return None  # undecided (converts to False in a Boolean expression)

    def mutate_column_stmt(self, source: Column, target: Column) -> Optional[str]:
        if source == target:
            return None

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
            statements.append(f"SET DEFAULT {target.default}")

        if source.identity and not target.identity:
            statements.append("DROP IDENTITY")
        elif not source.identity and target.identity:
            statements.append("ADD GENERATED BY DEFAULT AS IDENTITY")

        if statements:
            return ",\n".join(f"ALTER COLUMN {source.name} {s}" for s in statements)
        else:
            return None

    def mutate_table_stmt(self, source: Table, target: Table) -> Optional[str]:
        self.check_identity(source, target)

        statements: list[Optional[str]] = []

        create_columns = list(target.columns.difference(source.columns))
        common_columns = list(source.columns.intersection(target.columns))
        drop_columns = list(source.columns.difference(target.columns))
        migrated_columns: list[tuple[Column, Column]] = []

        # accumulate object creation statements
        create_stmts: list[str] = []

        # rename conflicting columns
        try:
            for pair in common_columns:
                source_column, target_column = pair
                if self.is_column_migrated(source_column, target_column):
                    statements.append(
                        source.alter_table_stmt([source_column.soft_drop_stmt()])
                    )
                    create_stmts.append(target_column.create_stmt())
                    migrated_columns.append(pair)
        except ColumnFormationError as e:
            raise TableFormationError(
                "failed to migrate columns in table", target.name
            ) from e

        # create new columns
        try:
            create_stmts.extend(column.create_stmt() for column in create_columns)
        except ColumnFormationError as e:
            raise TableFormationError(
                "failed to create columns in table", target.name
            ) from e

        if create_stmts:
            statements.append(source.alter_table_stmt(create_stmts))

        # accumulate object mutation statements
        alter_stmts: list[Optional[str]] = []

        # migrate data when data type changes
        try:
            for source_column, target_column in migrated_columns:
                statements.append(
                    self.migrate_column_stmt(
                        source, source_column, target, target_column
                    )
                )
                alter_stmts.append(source_column.hard_drop_stmt())
        except ColumnFormationError as e:
            raise TableFormationError(
                "failed to migrate columns in table", target.name
            ) from e

        # mutate existing columns as necessary
        try:
            for source_column, target_column in common_columns:
                alter_stmts.append(
                    self.mutate_column_stmt(source_column, target_column)
                )
        except ColumnFormationError as e:
            raise TableFormationError(
                "failed to update columns in table", target.name
            ) from e

        # remove deleted columns
        try:
            alter_stmts.extend(column.drop_stmt() for column in drop_columns)
        except ColumnFormationError as e:
            raise TableFormationError(
                "failed to drop columns in table", target.name
            ) from e

        alter_stmts.extend(
            f"DROP CONSTRAINT {constraint.name}"
            for constraint in source.constraints.difference(target.constraints)
            if constraint.is_alter_table()
        )

        # remove outdated constraints
        common_constraints = source.constraints.intersection(target.constraints)
        for source_constraint, target_constraint in common_constraints:
            if source_constraint != target_constraint:
                raise TableFormationError(
                    f"failed to mutate constraint `{source_constraint.name}`",
                    target.name,
                )

        # add new constraints
        alter_stmts.extend(
            f"ADD CONSTRAINT {constraint.spec}"
            for constraint in target.constraints.difference(source.constraints)
            if constraint.is_alter_table()
        )

        stmts = [s for s in alter_stmts if s is not None]
        if stmts:
            statements.append(source.alter_table_stmt(stmts))

        return join_or_none(statements)

    def mutate_namespace_stmt(
        self, source: Namespace, target: Namespace
    ) -> Optional[str]:
        self.check_identity(source, target)

        statements: StatementList = StatementList()

        # identify objects to create
        enum_create = list(target.enums.difference(source.enums))
        struct_create = list(target.structs.difference(source.structs))
        table_create = list(target.tables.difference(source.tables))

        # identify objects to drop
        enum_drop = list(source.enums.difference(target.enums))
        struct_drop = list(source.structs.difference(target.structs))
        table_drop = list(source.tables.difference(target.tables))
        enum_soft_deleted: list[EnumType] = []

        # create new objects
        statements.extend(enum.create_stmt() for enum in enum_create)
        statements.extend(struct.create_stmt() for struct in struct_create)

        for enum_type in enum_drop:
            for table in table_create:
                if enum_type.name != table.name:
                    continue
                statements.append(enum_type.soft_drop_stmt())
                enum_soft_deleted.append(enum_type)

        statements.extend(table.create_stmt() for table in table_create)
        statements.extend(table.add_constraints_stmt() for table in table_create)

        # populate new objects with data
        for enum_type in enum_drop:
            for table in table_create:
                if enum_type.name != table.name:
                    continue

                statements.append(self.migrate_enum_stmt(enum_type, table))

        # mutate existing object
        enum_mutate = list(source.enums.intersection(target.enums))
        statements.extend(
            self.mutate_enum_stmt(source_enum, target_enum)
            for source_enum, target_enum in enum_mutate
        )

        struct_mutate = list(source.structs.intersection(target.structs))
        statements.extend(
            self.mutate_struct_stmt(source_struct, target_struct)
            for source_struct, target_struct in struct_mutate
        )

        table_mutate = list(source.tables.intersection(target.tables))
        statements.extend(
            self.mutate_table_stmt(source_table, target_table)
            for source_table, target_table in table_mutate
        )

        # drop old objects
        if self.options.allow_drop_table:
            statements.extend(table.drop_stmt() for table in table_drop)

        if self.options.allow_drop_struct:
            statements.extend(struct.drop_stmt() for struct in struct_drop)

        if self.options.allow_drop_enum:
            for enum_type in enum_drop:
                statements.append(
                    enum_type.hard_drop_stmt()
                    if enum_type in enum_soft_deleted
                    else enum_type.drop_stmt()
                )

        return join_or_none(statements)

    def mutate_catalog_stmt(self, source: Catalog, target: Catalog) -> Optional[str]:
        statements: StatementList = StatementList()

        source.sort()
        target.sort()

        ns_create = list(target.namespaces.difference(source.namespaces))
        ns_drop = list(source.namespaces.difference(target.namespaces))
        ns_mutate = list(source.namespaces.intersection(target.namespaces))

        # create new namespaces
        statements.extend(namespace.create_schema_stmt() for namespace in ns_create)

        # create objects in each namespace added
        statements.extend(namespace.create_objects_stmt() for namespace in ns_create)

        # add new constraints
        statements.extend(namespace.add_constraints_stmt() for namespace in ns_create)

        # mutate existing namespaces
        statements.extend(
            self.mutate_namespace_stmt(source_ns, target_ns)
            for source_ns, target_ns in ns_mutate
        )

        # remove old constraints
        statements.extend(namespace.drop_constraints_stmt() for namespace in ns_drop)
        if self.options.allow_drop_namespace:
            # drop objects in each namespace removed
            statements.extend(namespace.drop_objects_stmt() for namespace in ns_drop)

            # drop old namespaces
            statements.extend(namespace.drop_schema_stmt() for namespace in ns_drop)

        return join_or_none(statements)
