from typing import Optional

from pysqlsync.formation.mutation import Mutator
from pysqlsync.formation.object_types import Column, ColumnFormationError


class MSSQLMutator(Mutator):
    def mutate_column_stmt(self, source: Column, target: Column) -> Optional[str]:
        if source.identity != target.identity:
            raise ColumnFormationError(
                f"operation not permitted; cannot add or drop identity property",
                source.name,
            )

        statements: list[str] = []
        if (
            source.data_type != target.data_type
            or source.nullable != target.nullable
            or source.default != target.default
        ):
            statements.append(f"ALTER COLUMN {source.name} {target.data_spec}")

        if statements:
            return ",\n".join(statements)
        else:
            return None
