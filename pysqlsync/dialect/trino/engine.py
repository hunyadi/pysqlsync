from pysqlsync.base import BaseConnection, BaseEngine, BaseGenerator, Explorer

from .connection import TrinoConnection
from .discovery import TrinoExplorer
from .generator import TrinoGenerator


class TrinoEngine(BaseEngine):
    @property
    def name(self) -> str:
        return "trino"

    def get_generator_type(self) -> type[BaseGenerator]:
        return TrinoGenerator

    def get_connection_type(self) -> type[BaseConnection]:
        return TrinoConnection

    def get_explorer_type(self) -> type[Explorer]:
        return TrinoExplorer
