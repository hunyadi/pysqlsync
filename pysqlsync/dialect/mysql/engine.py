from pysqlsync.base import BaseConnection, BaseEngine, BaseGenerator

from .connection import MySQLConnection
from .generator import MySQLGenerator


class MySQLEngine(BaseEngine):
    def get_generator_type(self) -> type[BaseGenerator]:
        return MySQLGenerator

    def get_connection_type(self) -> type[BaseConnection]:
        return MySQLConnection
