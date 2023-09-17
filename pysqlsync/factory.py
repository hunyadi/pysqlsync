import logging

from .base import BaseEngine

LOGGER = logging.getLogger("pysqlsync")

_engines: dict[str, type[BaseEngine]] = {}


def register_dialect(engine_name: str, engine_type: type[BaseEngine]) -> None:
    if engine_name in _engines:
        raise ValueError(f"engine already registered: {engine_name}")

    _engines[engine_name] = engine_type


def unregister_dialect(engine_name: str) -> None:
    _engines.pop(engine_name)


try:
    from .dialect.postgresql.engine import PostgreSQLEngine

except ImportError:
    LOGGER.debug("failed to import dialect: PostgreSQL", exc_info=True)
else:
    register_dialect("postgres", PostgreSQLEngine)
    register_dialect("postgresql", PostgreSQLEngine)


try:
    from .dialect.mysql.engine import MySQLEngine

except ImportError:
    LOGGER.debug("failed to import dialect: MySQL", exc_info=True)
else:
    register_dialect("mysql", MySQLEngine)


try:
    from .dialect.trino.engine import TrinoEngine


except ImportError:
    LOGGER.debug("failed to import dialect: Trino", exc_info=True)
else:
    register_dialect("trino", TrinoEngine)


def get_dialect(engine_name: str) -> BaseEngine:
    try:
        engine_type = _engines[engine_name]
    except KeyError:
        raise ValueError(f"unrecognized dialect: {engine_name}")
    else:
        return engine_type()
