from pysqlsync.base import BaseConnection, BaseEngine, BaseGenerator, Explorer

from .connection import OracleConnection
from .discovery import OracleExplorer
from .generator import OracleGenerator


class OracleEngine(BaseEngine):
    @property
    def name(self) -> str:
        return "oracle"

    def get_generator_type(self) -> type[BaseGenerator]:
        return OracleGenerator

    def get_connection_type(self) -> type[BaseConnection]:
        return OracleConnection

    def get_explorer_type(self) -> type[Explorer]:
        return OracleExplorer
