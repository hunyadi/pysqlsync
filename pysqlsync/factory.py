"""
pysqlsync: Synchronize schema and large volumes of data.

This module helps discover and register database dialects.

:see: https://github.com/hunyadi/pysqlsync
"""

import importlib
import importlib.resources
import logging
import re
import typing
from typing import Optional
from urllib.parse import parse_qs, unquote, urlparse

from strong_typing.inspection import get_module_classes

from .base import BaseConnection, BaseEngine, BaseGenerator, Explorer
from .connection import ConnectionParameters, ConnectionSSLMode

LOGGER = logging.getLogger("pysqlsync")

_engines: dict[str, BaseEngine] = {}


def register_dialect(engine_name: str, engine_factory: BaseEngine) -> None:
    """
    Dynamically registers a new database engine dialect.

    Connections strings to registered dialects are automatically recognized by pysqlsync.

    :param engine_name: The dialect name such as `postgres` or `mysql`.
    :param engine_factory: The engine factory to register, which creates connections, explorers and generators.
    """

    if engine_name in _engines:
        raise ValueError(f"engine already registered: {engine_name}")

    _engines[engine_name] = engine_factory


def unregister_dialect(engine_name: str) -> None:
    """
    Dynamically removes a database engine dialect.

    :param engine_name: The dialect name such as `postgres` or `mysql`.
    """

    _engines.pop(engine_name)


def get_dialect(engine_name: str) -> BaseEngine:
    "Looks up a database engine dialect based on its reference name."

    try:
        engine_factory = _engines[engine_name]
    except KeyError:
        raise ValueError(f"unrecognized dialect: {engine_name}")
    else:
        return engine_factory


def get_parameters(url: str) -> tuple[str, ConnectionParameters]:
    """
    Extracts dialect and connection parameters from a connection string URL.

    The connection string URL has the following structure:
    ```
    dialect://username:password@hostname:port/database
    ```

    For example,
    ```
    postgresql://root:%3C%3FYour%3AStrong%40Pass%2Fw0rd%3E@server.example.com:5432/public
    ```

    In the example above, dialect is `postgresql`, username is `root`, password is `<?Your:Strong@Pass/w0rd>`,
    hostname is `server.example.com`, port is `5432` and the database name is `public`. Note how components are
    URL-encoded (a.k.a. percent-encoded) to comply with RFC 3986.
    """

    parts = urlparse(url, allow_fragments=False)

    ssl: Optional[ConnectionSSLMode] = None
    if parts.query:
        query = parse_qs(parts.query, strict_parsing=True)
        if "ssl" in query:
            if len(query["ssl"]) != 1:
                raise ValueError(
                    "only a single `ssl` parameter is permitted in a connection string"
                )
            ssl_mode = query["ssl"][0]
            for v in ConnectionSSLMode.__members__.values():
                if ssl_mode == v.value:
                    ssl = v
                    break
            else:
                raise ValueError(f"unsupported SSL mode: {ssl_mode}")

    return parts.scheme, ConnectionParameters(
        host=parts.hostname,
        port=parts.port,
        username=unquote(parts.username) if parts.username else None,
        password=unquote(parts.password) if parts.password else None,
        database=parts.path.lstrip("/") if parts.path else None,
        ssl=ssl,
    )


class UnavailableEngine(BaseEngine):
    _name: str
    _module: str

    @property
    def name(self) -> str:
        return self._name

    def __init__(self, name: str, module: str) -> None:
        self._name = name
        self._module = module

    def get_generator_type(self) -> type[BaseGenerator]:
        self._raise_error()

    def get_connection_type(self) -> type[BaseConnection]:
        self._raise_error()

    def get_explorer_type(self) -> type[Explorer]:
        self._raise_error()

    def _raise_error(self) -> typing.NoReturn:
        raise RuntimeError(
            f"missing dependency: `{self._module}`; you may need to run `pip install pysqlsync[{self._name}]`"
        )


def discover_dialects() -> None:
    "Discovers database engine dialects bundled with this package."

    resources = importlib.resources.files(__package__).joinpath("dialect").iterdir()
    for resource in resources:
        if resource.name.startswith((".", "__")) or not resource.is_dir():
            continue

        # run a preliminary check importing only the module `dependency`;
        # if the check fails, it indicates that required dependencies have not been installed (e.g. database driver)
        try:
            module = importlib.import_module(
                f".dialect.{resource.name}.dependency", package=__package__
            )
        except ModuleNotFoundError as e:
            LOGGER.debug(
                "skipping dialect `%s`: missing dependency: `%s`; "
                "you may need to run `pip install pysqlsync[%s]`",
                resource.name,
                e.name,
                resource.name,
            )
            register_dialect(
                resource.name, UnavailableEngine(resource.name, e.name or "")
            )
            continue

        # import the module `engine`, which acts as an entry point to connector, generator and explorer functionality
        module = importlib.import_module(
            f".dialect.{resource.name}.engine", package=__package__
        )
        classes = [
            cls
            for cls in get_module_classes(module)
            if re.match(r"^\w+Engine$", cls.__name__)
        ]
        engine_type = typing.cast(type[BaseEngine], classes.pop())
        engine_factory = engine_type()
        LOGGER.info(
            "found dialect `%s` defined by `%s`",
            engine_factory.name,
            engine_type.__name__,
        )
        register_dialect(engine_factory.name, engine_factory)


discover_dialects()
