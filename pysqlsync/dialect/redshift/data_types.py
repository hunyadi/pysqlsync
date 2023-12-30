from pysqlsync.model.data_types import SqlVariableBinaryType


class RedshiftVariableBinaryType(SqlVariableBinaryType):
    def __str__(self) -> str:
        if self.storage is not None:
            storage = f"({self.storage})"
        else:
            storage = ""
        return f"varbyte{storage}"
