from pysqlsync.formation.object_types import Column, FormationError, ObjectFactory
from pysqlsync.model.data_types import constant


class MSSQLColumn(Column):
    @property
    def data_spec(self) -> str:
        nullable = " NOT NULL" if not self.nullable else ""
        default = (
            f" DEFAULT {constant(self.default)}" if self.default is not None else ""
        )
        identity = " IDENTITY" if self.identity else ""
        return f"{self.data_type}{nullable}{default}{identity}"

    def mutate_column_stmt(target: "MSSQLColumn", source: Column) -> list[str]:
        if source.identity != target.identity:
            raise FormationError(
                "operation not permitted; cannot add or drop identity property"
            )
        return super().mutate_column_stmt(source)


class MSSQLObjectFactory(ObjectFactory):
    @property
    def column_class(self) -> type[Column]:
        return MSSQLColumn
