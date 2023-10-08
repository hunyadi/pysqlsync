from pysqlsync.base import BaseConnection, BaseEngine, BaseGenerator, Explorer

from .connection import MySQLConnection
from .discovery import MySQLExplorer
from .generator import MySQLGenerator


class MySQLEngine(BaseEngine):
    @property
    def name(self) -> str:
        return "mysql"

    def get_generator_type(self) -> type[BaseGenerator]:
        return MySQLGenerator

    def get_connection_type(self) -> type[BaseConnection]:
        return MySQLConnection

    def get_explorer_type(self) -> type[Explorer]:
        return MySQLExplorer
