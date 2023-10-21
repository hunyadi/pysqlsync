from pysqlsync.base import BaseConnection, BaseEngine, BaseGenerator, Explorer

from .connection import MSSQLConnection
from .generator import MSSQLGenerator
from .discovery import MSSQLExplorer

class MSSQLEngine(BaseEngine):
    @property
    def name(self) -> str:
        return "mssql"

    def get_generator_type(self) -> type[BaseGenerator]:
        return MSSQLGenerator

    def get_connection_type(self) -> type[BaseConnection]:
        return MSSQLConnection

    def get_explorer_type(self) -> type[Explorer]:
        return MSSQLExplorer
