import typing
from typing import Optional

from pysqlsync.formation.mutation import Mutator
from pysqlsync.formation.object_types import Column, StatementList, Table, join_or_none
from pysqlsync.model.data_types import quote

from .object_types import MySQLColumn, MySQLTable


class MySQLMutator(Mutator):
    def mutate_table_stmt(
        self, source_table: Table, target_table: Table
    ) -> Optional[str]:
        source = typing.cast(MySQLTable, source_table)
        target = typing.cast(MySQLTable, target_table)

        statements: StatementList = StatementList()
        statements.append(super().mutate_table_stmt(source, target))

        source_desc = source.short_description
        target_desc = target.short_description

        if source_desc is None:
            if target_desc is not None:
                statements.append(
                    f"ALTER TABLE {target.name} COMMENT = {quote(target_desc)};"
                )
        else:
            if target_desc is None:
                statements.append(f"ALTER TABLE {target.name} COMMENT = {quote('')};")
            elif source_desc != target_desc:
                statements.append(
                    f"ALTER TABLE {target.name} COMMENT = {quote(target_desc)};"
                )

        return join_or_none(statements)

    def mutate_column_stmt(
        self, source_column: Column, target_column: Column
    ) -> Optional[str]:
        source = typing.cast(MySQLColumn, source_column)
        target = typing.cast(MySQLColumn, target_column)

        if (
            source.data_type != target.data_type
            or source.nullable != target.nullable
            or source.default != target.default
            or source.identity != target.identity
            or source.comment != target.comment
        ):
            return f"MODIFY COLUMN {source.name} {target.data_spec}"
        else:
            return None
