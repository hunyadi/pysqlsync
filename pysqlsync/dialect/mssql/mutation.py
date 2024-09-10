import typing
from typing import Optional

from pysqlsync.formation.mutation import Mutator
from pysqlsync.formation.object_types import (
    Column,
    ColumnFormationError,
    StatementList,
    Table,
    join_or_none,
)

from .object_types import MSSQLColumn


class MSSQLMutator(Mutator):
    def mutate_column_stmt(self, source: Column, target: Column) -> Optional[str]:
        if source.identity != target.identity:
            raise ColumnFormationError(
                "operation not permitted; cannot add or drop identity property",
                source.name,
            )

        if source.data_type != target.data_type or source.nullable != target.nullable:
            nullable = " NOT NULL" if not target.nullable else ""
            return f"ALTER COLUMN {source.name} {target.data_type}{nullable}"
        else:
            return None

    def mutate_table_stmt(self, source: Table, target: Table) -> Optional[str]:
        statements = StatementList()

        constraints: list[str] = []
        common_columns = source.columns.intersection(target.columns)
        for source_column, target_column in common_columns:
            source_def = source_column.default
            target_def = target_column.default

            if source_def == target_def:
                continue

            name = typing.cast(MSSQLColumn, source_column).default_constraint_name()
            if source_def is not None:
                constraints.append(f"DROP CONSTRAINT {name}")
            if target_def is not None:
                constraints.append(
                    f"ADD CONSTRAINT {name} DEFAULT {target_def} FOR {source_column.name}"
                )

        if constraints:
            statements.append(source.alter_table_stmt(constraints))

        statements.append(super().mutate_table_stmt(source, target))
        return join_or_none(statements)
