from pysqlsync.base import BaseConnection, BaseEngine, BaseGenerator, Explorer

from .connection import PostgreSQLConnection
from .discovery import PostgreSQLExplorer
from .generator import PostgreSQLGenerator


class PostgreSQLEngine(BaseEngine):
    @property
    def name(self) -> str:
        return "postgresql"

    def get_generator_type(self) -> type[BaseGenerator]:
        return PostgreSQLGenerator

    def get_connection_type(self) -> type[BaseConnection]:
        return PostgreSQLConnection

    def get_explorer_type(self) -> type[Explorer]:
        return PostgreSQLExplorer
