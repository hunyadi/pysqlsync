from pysqlsync.base import BaseConnection, BaseEngine, BaseGenerator, Explorer
from pysqlsync.formation.discovery import AnsiReflection

from .connection import MySQLConnection
from .generator import MySQLGenerator


class MySQLEngine(BaseEngine):
    def get_generator_type(self) -> type[BaseGenerator]:
        return MySQLGenerator

    def get_connection_type(self) -> type[BaseConnection]:
        return MySQLConnection

    def get_explorer_type(self) -> type[Explorer]:
        return AnsiReflection
