from dataclasses import dataclass

from ..model.id_types import LocalId, SupportsQualifiedId
from .object_types import ConstraintReference, ForeignConstraint, UniqueConstraint


class UniqueFactory:
    "Builds a list of unique constraints from a table of constraint names and columns."

    name_to_columns: dict[str, list[LocalId]]

    def __init__(self) -> None:
        self.name_to_columns = {}

    def add(self, constraint_name: str, column: LocalId) -> None:
        self.name_to_columns.setdefault(constraint_name, []).append(column)

    def fetch(self) -> list[UniqueConstraint]:
        return [
            UniqueConstraint(LocalId(name), tuple(columns))
            for name, columns in self.name_to_columns.items()
        ]


@dataclass
class ColumnPair:
    source: LocalId
    target: LocalId


@dataclass
class ConstraintLink:
    target_table: SupportsQualifiedId
    column_pairs: list[ColumnPair]

    def __init__(self, target_table: SupportsQualifiedId) -> None:
        self.target_table = target_table
        self.column_pairs = []


class ForeignFactory:
    "Builds a list of foreign key constraints from a table of constraint names, source and target columns."

    name_to_constraint: dict[str, ConstraintLink]

    def __init__(self) -> None:
        self.target_table = None
        self.name_to_constraint = {}

    def add(
        self,
        constraint_name: str,
        source_column: LocalId,
        target_table: SupportsQualifiedId,
        target_column: LocalId,
    ) -> None:
        link = self.name_to_constraint.get(constraint_name)
        if link is not None:
            if target_table != link.target_table:
                raise ValueError("inconsistent target table for foreign constraint")
        else:
            link = ConstraintLink(target_table)
            self.name_to_constraint[constraint_name] = link

        link.column_pairs.append(ColumnPair(source_column, target_column))

    def fetch(self) -> list[ForeignConstraint]:
        if not self.name_to_constraint:
            return []

        return [
            ForeignConstraint(
                LocalId(name),
                tuple(item.source for item in link.column_pairs),
                ConstraintReference(
                    link.target_table,
                    tuple(item.target for item in link.column_pairs),
                ),
            )
            for name, link in self.name_to_constraint.items()
        ]
