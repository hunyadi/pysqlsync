from pysqlsync.formation.object_types import ObjectFactory, Table


class OracleTable(Table):
    def alter_table_stmt(self, statements: list[str]) -> str:
        return "\n".join(
            f"ALTER TABLE {self.name} {statement};" for statement in statements
        )


class OracleObjectFactory(ObjectFactory):
    @property
    def table_class(self) -> type[Table]:
        return OracleTable
