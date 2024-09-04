"""
pysqlsync: Synchronize schema and large volumes of data.

This module helps convert between data-class, dictionary and tuple representation of result-sets.

:see: https://github.com/hunyadi/pysqlsync
"""

import typing
from collections.abc import Sequence
from typing import Any, Iterable, TypeVar

from strong_typing.inspection import DataclassInstance, is_dataclass_type

D = TypeVar("D", bound=DataclassInstance)
T = TypeVar("T")


def resultset_unwrap_dict(
    signature: type[D], records: Iterable[dict[str, Any]]
) -> list[D]:
    """
    Converts a result-set into a list of data-class instances.

    :param signature: A data-class type.
    :param records: The result-set whose rows to convert.
    """

    if not is_dataclass_type(signature):
        raise TypeError(
            f"expected: data-class type as result-set signature; got: {signature}"
        )

    return [
        signature(**{name: value for name, value in record.items()})  # type: ignore
        for record in records
    ]


def resultset_unwrap_object(signature: type[D], records: Iterable[Any]) -> list[D]:
    """
    Converts a result-set into a list of data-class instances.

    :param signature: A data-class type.
    :param records: The result-set whose rows to convert.
    """

    if not is_dataclass_type(signature):
        raise TypeError(
            f"expected: data-class type as result-set signature; got: {signature}"
        )

    names = [name for name in signature.__dataclass_fields__.keys()]
    return [
        signature(**{name: record.__getattribute__(name) for name in names}) for record in records  # type: ignore
    ]


def resultset_unwrap_tuple(
    signature: type[T], records: Iterable[Sequence[Any]]
) -> list[T]:
    """
    Converts a result-set into a list of tuples, or a list of simple types (as appropriate).

    :param signature: A tuple type, or a simple type (e.g. `bool` or `str`).
    :param records: The result-set whose rows to convert.
    """

    if signature in [bool, int, float, str]:
        scalar_results: list[T] = []

        # check result shape
        it = iter(records)
        try:
            item = next(it)
        except StopIteration:
            return []
        if len(item) != 1:
            raise ValueError(
                f"invalid number of columns, expected: 1; got: {len(item)}"
            )
        scalar_results.append(item[0])
        while True:
            try:
                item = next(it)
            except StopIteration:
                return scalar_results
            scalar_results.append(item[0])

    origin_type = typing.get_origin(signature)
    if origin_type is tuple:
        origin_args = typing.get_args(signature)
        results: list[T] = []

        # check result shape
        it = iter(records)
        try:
            item = next(it)
        except StopIteration:
            return []
        if len(item) != len(origin_args):
            raise ValueError(
                f"invalid number of columns, expected: {len(origin_args)}; got: {len(item)}"
            )

        if isinstance(item, tuple):
            results.append(item)  # type: ignore
            while True:
                try:
                    item = next(it)
                except StopIteration:
                    return results
                results.append(item)  # type: ignore
        else:
            results.append(tuple(item))  # type: ignore
            while True:
                try:
                    item = next(it)
                except StopIteration:
                    return results
                results.append(tuple(item))  # type: ignore

    raise TypeError(
        f"expected: tuple or simple type as result-set signature; got: {signature}"
    )
