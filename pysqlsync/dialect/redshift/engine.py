from pysqlsync.base import BaseConnection, BaseEngine, BaseGenerator, Explorer

from ..postgresql.discovery import PostgreSQLExplorer
from .connection import RedshiftConnection
from .generator import RedshiftGenerator


class RedshiftEngine(BaseEngine):
    @property
    def name(self) -> str:
        return "redshift"

    def get_generator_type(self) -> type[BaseGenerator]:
        return RedshiftGenerator

    def get_connection_type(self) -> type[BaseConnection]:
        return RedshiftConnection

    def get_explorer_type(self) -> type[Explorer]:
        return PostgreSQLExplorer
