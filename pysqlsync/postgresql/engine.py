from .connection import Connection
from .generator import Generator
from ..base import BaseConnection, BaseEngine, BaseGenerator


class PostgreSQLEngine(BaseEngine):
    def get_generator_type(self) -> type[BaseGenerator]:
        return Generator

    def get_connection_type(self) -> type[BaseConnection]:
        return Connection
