from pysqlsync.base import BaseConnection, BaseEngine, BaseGenerator, Explorer

from ..postgresql.discovery import PostgreSQLExplorer
from ..postgresql.generator import PostgreSQLGenerator
from .connection import RedshiftConnection


class PostgreSQLEngine(BaseEngine):
    @property
    def name(self) -> str:
        return "redshift"

    def get_generator_type(self) -> type[BaseGenerator]:
        return PostgreSQLGenerator

    def get_connection_type(self) -> type[BaseConnection]:
        return RedshiftConnection

    def get_explorer_type(self) -> type[Explorer]:
        return PostgreSQLExplorer
