from pysqlsync.base import BaseConnection, BaseEngine, BaseGenerator, Explorer

from .generator import SnowflakeGenerator


class SnowflakeEngine(BaseEngine):
    @property
    def name(self) -> str:
        return "snowflake"

    def get_generator_type(self) -> type[BaseGenerator]:
        return SnowflakeGenerator

    def get_connection_type(self) -> type[BaseConnection]:
        raise NotImplementedError()

    def get_explorer_type(self) -> type[Explorer]:
        raise NotImplementedError()
