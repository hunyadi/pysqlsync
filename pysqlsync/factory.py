import importlib
import importlib.resources
import logging
import os.path
import re
import typing

from strong_typing.inspection import get_module_classes

from .base import BaseEngine

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


def discover_dialects() -> None:
    "Discovers database engine dialects bundled with this package."

    resources = importlib.resources.files(__package__).joinpath("dialect").iterdir()
    for resource in resources:
        if resource.name.startswith((".", "__")) or not resource.is_dir():
            continue

        try:
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

        except ImportError:
            pass


discover_dialects()
