from ..base import BaseConnection, BaseEngine, BaseGenerator
from .connection import TrinoConnection
from .generator import TrinoGenerator


class TrinoEngine(BaseEngine):
    def get_generator_type(self) -> type[BaseGenerator]:
        return TrinoGenerator

    def get_connection_type(self) -> type[BaseConnection]:
        return TrinoConnection
