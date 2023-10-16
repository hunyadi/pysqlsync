from pysqlsync.base import BaseConnection, BaseEngine, BaseGenerator, Explorer
from pysqlsync.formation.discovery import AnsiReflection

from .generator import MSSQLGenerator


class MSSQLEngine(BaseEngine):
    @property
    def name(self) -> str:
        return "mssql"

    def get_generator_type(self) -> type[BaseGenerator]:
        return MSSQLGenerator

    def get_connection_type(self) -> type[BaseConnection]:
        raise NotImplementedError()

    def get_explorer_type(self) -> type[Explorer]:
        return AnsiReflection
