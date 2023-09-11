from ..base import BaseConnection, BaseEngine, BaseGenerator
from .connection import PostgreSQLConnection
from .generator import PostgreSQLGenerator


class PostgreSQLEngine(BaseEngine):
    def get_generator_type(self) -> type[BaseGenerator]:
        return PostgreSQLGenerator

    def get_connection_type(self) -> type[BaseConnection]:
        return PostgreSQLConnection
