from typing import Optional

from pysqlsync.formation.mutation import Mutator
from pysqlsync.formation.object_types import (
    StatementList,
    StructType,
    Table,
    join_or_none,
)

from .object_types import sql_quoted_string


class PostgreSQLMutator(Mutator):
    def mutate_table_stmt(self, source: Table, target: Table) -> Optional[str]:
        statements: StatementList = StatementList()
        statements.append(super().mutate_table_stmt(source, target))

        for target_column in target.columns.values():
            source_column = source.columns.get(target_column.name.id)

            source_desc = (
                source_column.description if source_column is not None else None
            )
            target_desc = target_column.description

            if target_desc is None:
                if source_desc is not None:
                    statements.append(
                        f"COMMENT ON COLUMN {target.name}.{target_column.name} IS NULL;"
                    )
            else:
                if source_desc != target_desc:
                    statements.append(
                        f"COMMENT ON COLUMN {target.name}.{target_column.name} IS {sql_quoted_string(target_desc)};"
                    )

        if target.description is None:
            if source.description is not None:
                statements.append(f"COMMENT ON TABLE {target.name} IS NULL;")
        else:
            if source.description != target.description:
                statements.append(
                    f"COMMENT ON TABLE {target.name} IS {sql_quoted_string(target.description)};"
                )

        return join_or_none(statements)

    def mutate_struct_stmt(
        self, source: StructType, target: StructType
    ) -> Optional[str]:
        statements: StatementList = StatementList()
        statements.append(super().mutate_struct_stmt(source, target))

        if target.description is None:
            if source.description is not None:
                statements.append(f"COMMENT ON TYPE {target.name} IS NULL;")
        else:
            if source.description != target.description:
                statements.append(
                    f"COMMENT ON TYPE {target.name} IS {sql_quoted_string(target.description)};"
                )

        return join_or_none(statements)
