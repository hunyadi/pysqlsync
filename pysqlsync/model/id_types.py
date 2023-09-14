import abc
from dataclasses import dataclass
from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class SupportsLocalId(Protocol):
    __slots__ = ()

    @abc.abstractproperty
    def local_id(self) -> str:
        ...


@runtime_checkable
class SupportsName(Protocol):
    __slots__ = ()

    @abc.abstractproperty
    def name(self) -> SupportsLocalId:
        ...


@dataclass(frozen=True)
class LocalId:
    id: str

    @property
    def compact_id(self) -> str:
        return self.id

    @property
    def local_id(self) -> str:
        return self.id

    def __str__(self) -> str:
        "Quotes an identifier to be embedded in a SQL statement."

        return '"' + self.id.replace('"', '""') + '"'


@dataclass(frozen=True)
class PrefixedId:
    namespace: Optional[str]
    id: str

    @property
    def compact_id(self) -> str:
        return f"{self.namespace}__{self.id}"

    @property
    def local_id(self) -> str:
        return self.compact_id

    def __str__(self) -> str:
        "Quotes a qualified identifier to be embedded in a SQL statement."

        if self.namespace is not None:
            return (
                '"'
                + self.namespace.replace('"', '""')
                + "__"
                + self.id.replace('"', '""')
                + '"'
            )
        else:
            return '"' + self.id.replace('"', '""') + '"'


@dataclass(frozen=True)
class QualifiedId:
    namespace: Optional[str]
    id: str

    @property
    def compact_id(self) -> str:
        return f"{self.namespace}.{self.id}"

    @property
    def local_id(self) -> str:
        return self.id

    def __str__(self) -> str:
        "Quotes a qualified identifier to be embedded in a SQL statement."

        if self.namespace is not None:
            return (
                '"'
                + self.namespace.replace('"', '""')
                + '"."'
                + self.id.replace('"', '""')
                + '"'
            )
        else:
            return '"' + self.id.replace('"', '""') + '"'
