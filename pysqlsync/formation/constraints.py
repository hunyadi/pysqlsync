from dataclasses import dataclass
from typing import Optional

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


class ForeignFactory:
    "Builds a list of foreign key constraints from a table of constraint names, source and target columns."

    target_table: Optional[SupportsQualifiedId]
    name_to_items: dict[str, list[ColumnPair]]

    def __init__(self) -> None:
        self.target_table = None
        self.name_to_items = {}

    def add(
        self,
        constraint_name: str,
        source_column: LocalId,
        target_table: SupportsQualifiedId,
        target_column: LocalId,
    ) -> None:
        if self.target_table is not None and self.target_table != target_table:
            raise ValueError("inconsistent target table for foreign constraint")
        else:
            self.target_table = target_table

        self.name_to_items.setdefault(constraint_name, []).append(
            ColumnPair(source_column, target_column)
        )

    def fetch(self) -> list[ForeignConstraint]:
        if not self.name_to_items:
            return []

        if self.target_table is None:
            raise ValueError("missing target table for foreign constraint")

        return [
            ForeignConstraint(
                LocalId(name),
                tuple(item.source for item in items),
                ConstraintReference(
                    self.target_table,
                    tuple(item.target for item in items),
                ),
            )
            for name, items in self.name_to_items.items()
        ]
