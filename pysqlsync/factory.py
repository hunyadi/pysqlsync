from .base import BaseEngine


def get_engine(engine_name: str) -> BaseEngine:
    if engine_name == "postgresql":
        from .postgresql.engine import PostgreSQLEngine

        return PostgreSQLEngine()

    elif engine_name == "trino":
        from .trino.engine import TrinoEngine

        return TrinoEngine()

    else:
        raise TypeError(f"unrecognized engine type: {engine_name}")
