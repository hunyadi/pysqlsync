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
from urllib.parse import urlparse

from strong_typing.inspection import get_module_classes

from .base import (
    BaseConnection,
    BaseEngine,
    BaseGenerator,
    ConnectionParameters,
    Explorer,
)

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
    parts = urlparse(url)
    return parts.scheme, ConnectionParameters(
        host=parts.hostname,
        port=parts.port,
        username=parts.username,
        password=parts.password,
        database=parts.path.lstrip("/") if parts.path else None,
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
                f"skipping dialect `{resource.name}`: missing dependency: `{e.name}`; "
                f"you may need to run `pip install pysqlsync[{resource.name}]`"
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
            f"found dialect `{engine_factory.name}` defined by `{engine_type.__name__}`"
        )
        register_dialect(engine_factory.name, engine_factory)


discover_dialects()
