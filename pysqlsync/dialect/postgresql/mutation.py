from typing import Optional

from pysqlsync.formation.mutation import Mutator
from pysqlsync.formation.object_types import (
    Column,
    EnumType,
    StatementList,
    StructType,
    Table,
    deleted,
    join_or_none,
)
from pysqlsync.model.data_types import SqlIntegerType, SqlUserDefinedType, quote
from pysqlsync.model.id_types import LocalId
from pysqlsync.util.typing import override

from .object_types import sql_quoted_string


class PostgreSQLMutator(Mutator):
    @override
    def is_column_migrated(self, source: Column, target: Column) -> Optional[bool]:
        is_migrated = super().is_column_migrated(source, target)
        if is_migrated is not None:
            return is_migrated

        if isinstance(target.data_type, SqlIntegerType):
            # PostgreSQL defines a separate type for each enumeration type
            if isinstance(source.data_type, SqlUserDefinedType):
                return True

        return None  # undecided (converts to False in a Boolean expression)

    def migrate_enum_stmt(self, enum_type: EnumType, table: Table) -> Optional[str]:
        enum_values = ", ".join(f"({quote(v)})" for v in enum_type.values)
        return f'INSERT INTO {table.name} ("value") VALUES {enum_values} ON CONFLICT ("value") DO NOTHING;'

    def migrate_column_stmt(
        self, source_table: Table, source: Column, target_table: Table, target: Column
    ) -> Optional[str]:
        ref = target_table.get_constraint(target.name)
        return (
            f"UPDATE {source_table.name} data_table\n"
            f'SET {target.name} = enum_table."id"\n'
            f"FROM {ref.table} enum_table\n"
            f'WHERE data_table.{LocalId(deleted(source.name.id))}::VARCHAR = enum_table."value";'
        )

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
