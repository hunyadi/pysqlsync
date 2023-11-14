from typing import Optional

from pysqlsync.formation.mutation import Mutator
from pysqlsync.formation.object_types import Column, ColumnFormationError


class MSSQLMutator(Mutator):
    def mutate_column_stmt(self, source: Column, target: Column) -> Optional[str]:
        if source.identity != target.identity:
            raise ColumnFormationError(
                "operation not permitted; cannot add or drop identity property",
                source.name,
            )

        if (
            source.data_type != target.data_type
            or source.nullable != target.nullable
            or source.default != target.default
        ):
            return f"ALTER COLUMN {source.name} {target.data_spec}"
        else:
            return None
