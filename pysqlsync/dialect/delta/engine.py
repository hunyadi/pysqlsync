from pysqlsync.base import BaseConnection, BaseEngine, BaseGenerator, Explorer

from .generator import DeltaGenerator


class DeltaEngine(BaseEngine):
    @property
    def name(self) -> str:
        return "delta"

    def get_generator_type(self) -> type[BaseGenerator]:
        return DeltaGenerator

    def get_connection_type(self) -> type[BaseConnection]:
        raise NotImplementedError()

    def get_explorer_type(self) -> type[Explorer]:
        raise NotImplementedError()
