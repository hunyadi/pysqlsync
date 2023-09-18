import abc
from dataclasses import dataclass
from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class SupportsQuotedId(Protocol):
    __slots__ = ()

    @abc.abstractproperty
    def quoted_id(self) -> str:
        "A fully-quoted identifier."
        ...


@runtime_checkable
class SupportsLocalId(Protocol):
    __slots__ = ()

    @abc.abstractproperty
    def local_id(self) -> str:
        "The component of an identifier to be used in a local context, e.g. columns of a table."
        ...

    @abc.abstractproperty
    def quoted_id(self) -> str:
        "A fully-quoted identifier."
        ...


@runtime_checkable
class SupportsQualifiedId(Protocol):
    __slots__ = ()

    @abc.abstractproperty
    def scope_id(self) -> Optional[str]:
        ...

    @abc.abstractproperty
    def local_id(self) -> str:
        "The component of an identifier to be used in a local context, e.g. columns of a table."
        ...

    @abc.abstractproperty
    def compact_id(self) -> str:
        "An unquoted identifier."
        ...

    @abc.abstractproperty
    def quoted_id(self) -> str:
        "A fully-quoted identifier."
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
    def local_id(self) -> str:
        return self.id

    @property
    def quoted_id(self) -> str:
        return '"' + self.id.replace('"', '""') + '"'

    def __str__(self) -> str:
        "Quotes an identifier to be embedded in a SQL statement."

        return self.quoted_id


@dataclass(frozen=True)
class PrefixedId:
    namespace: Optional[str]
    id: str

    @property
    def scope_id(self) -> Optional[str]:
        return None

    @property
    def local_id(self) -> str:
        return f"{self.namespace}__{self.id}"

    @property
    def compact_id(self) -> str:
        return self.local_id

    @property
    def quoted_id(self) -> str:
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

    def __str__(self) -> str:
        "Quotes a qualified identifier to be embedded in a SQL statement."

        return self.quoted_id


@dataclass(frozen=True)
class QualifiedId:
    namespace: Optional[str]
    id: str

    @property
    def scope_id(self) -> Optional[str]:
        return self.namespace

    @property
    def local_id(self) -> str:
        return self.id

    @property
    def compact_id(self) -> str:
        if self.namespace is not None:
            return f"{self.namespace}.{self.id}"
        else:
            return self.id

    @property
    def quoted_id(self) -> str:
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

    def __str__(self) -> str:
        "Quotes a qualified identifier to be embedded in a SQL statement."

        return self.quoted_id


@dataclass(frozen=True)
class GlobalId:
    id: str

    @property
    def scope_id(self) -> Optional[str]:
        return None

    @property
    def local_id(self) -> str:
        return self.id

    @property
    def compact_id(self) -> str:
        return self.id

    @property
    def quoted_id(self) -> str:
        return self.id

    def __str__(self) -> str:
        return self.id
