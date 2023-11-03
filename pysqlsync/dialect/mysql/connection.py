import logging
import sys
import typing
from typing import Optional, TypeVar

from strong_typing.inspection import DataclassInstance
from typing_extensions import override

from pysqlsync.base import BaseConnection, BaseContext
from pysqlsync.model.data_types import escape_like
from pysqlsync.model.id_types import LocalId

D = TypeVar("D", bound=DataclassInstance)
T = TypeVar("T")

LOGGER = logging.getLogger("pysqlsync.mysql")


class MySQLContextBase(BaseContext):
    @override
    async def current_schema(self) -> Optional[str]:
        return None

    @override
    async def drop_schema(self, namespace: LocalId) -> None:
        LOGGER.debug(f"drop schema: {namespace}")

        tables = await self.query_all(
            str,
            "SELECT table_name\n"
            "FROM information_schema.tables\n"
            f"WHERE table_schema = DATABASE() AND table_name LIKE '{escape_like(namespace.id, '~')}~_~_%' ESCAPE '~';",
        )
        if tables:
            table_list = ", ".join(str(LocalId(table)) for table in tables)
            await self.execute(f"DROP TABLE IF EXISTS {table_list};")


if typing.TYPE_CHECKING:

    class MySQLConnection(BaseConnection):
        pass

    class MySQLContext(MySQLContextBase):
        pass

else:
    if not hasattr(sys.modules[__name__], "MySQLConnection"):
        try:
            from .impl.aiomysql import MySQLConnection as MySQLConnection
            from .impl.aiomysql import MySQLContext as MySQLContext

            LOGGER.info("aiomysql is installed.")
        except ImportError:
            pass

    if not hasattr(sys.modules[__name__], "MySQLConnection"):
        try:
            from .impl.mysqlclient import MySQLConnection as MySQLConnection
            from .impl.mysqlclient import MySQLContext as MySQLContext

            LOGGER.info("mysqlclient is installed.")
        except ImportError:
            pass
