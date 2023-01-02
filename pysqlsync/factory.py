from .base import BaseEngine


def get_engine(engine_name: str) -> BaseEngine:
    if engine_name == "postgresql":
        from .postgresql.engine import PostgreSQLEngine

        return PostgreSQLEngine()

    else:
        raise TypeError(f"unrecognized engine type: {engine_name}")
