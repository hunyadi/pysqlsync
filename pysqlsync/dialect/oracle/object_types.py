from pysqlsync.formation.object_types import ObjectFactory, Table


class OracleTable(Table):
    def alter_table_stmt(self, statements: list[str]) -> str:
        return "\n".join(
            f"ALTER TABLE {self.name} {statement};" for statement in statements
        )

    def drop_if_exists_stmt(self) -> str:
        return (
            "BEGIN\n"
            f"    EXECUTE IMMEDIATE 'DROP TABLE {self.name}';\n"
            "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;\n"
            "END;"
        )


class OracleObjectFactory(ObjectFactory):
    @property
    def table_class(self) -> type[Table]:
        return OracleTable
