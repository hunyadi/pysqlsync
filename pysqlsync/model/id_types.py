import abc
from dataclasses import dataclass
from typing import Optional, Protocol, runtime_checkable

ID_QUOTE_CHAR = '"'


def quote_id(name: str) -> str:
    return name.replace(ID_QUOTE_CHAR, 2 * ID_QUOTE_CHAR)


@runtime_checkable
class SupportsQuotedId(Protocol):
    __slots__ = ()

    @property
    @abc.abstractmethod
    def quoted_id(self) -> str:
        "A fully-quoted identifier."
        ...


@runtime_checkable
class SupportsLocalId(Protocol):
    __slots__ = ()

    @property
    @abc.abstractmethod
    def local_id(self) -> str:
        "Unquoted component of an identifier to be used in a local context, e.g. columns of a table."
        ...

    @property
    @abc.abstractmethod
    def quoted_id(self) -> str:
        "A fully-quoted identifier."
        ...


@runtime_checkable
class SupportsQualifiedId(Protocol):
    __slots__ = ()

    @property
    @abc.abstractmethod
    def scope_id(self) -> Optional[str]:
        "Unquoted scope identifier."
        ...

    @property
    @abc.abstractmethod
    def local_id(self) -> str:
        "Unquoted component of an identifier to be used in a local context, e.g. columns of a table."
        ...

    @property
    @abc.abstractmethod
    def compact_id(self) -> str:
        "An unquoted composite identifier."
        ...

    @property
    @abc.abstractmethod
    def quoted_id(self) -> str:
        "A fully-quoted identifier."
        ...

    def rename(self, id: str) -> "SupportsQualifiedId": ...


@runtime_checkable
class SupportsName(Protocol):
    __slots__ = ()

    @property
    @abc.abstractmethod
    def name(self) -> SupportsLocalId: ...


@dataclass(frozen=True)
class LocalId:
    id: str

    @property
    def local_id(self) -> str:
        "Unquoted identifier."

        return self.id

    @property
    def quoted_id(self) -> str:
        return ID_QUOTE_CHAR + quote_id(self.id) + ID_QUOTE_CHAR

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
        return f"{self.namespace}__{self.id}" if self.namespace is not None else self.id

    @property
    def compact_id(self) -> str:
        return self.local_id

    @property
    def quoted_id(self) -> str:
        if self.namespace is not None:
            return (
                ID_QUOTE_CHAR
                + quote_id(self.namespace)
                + "__"
                + quote_id(self.id)
                + ID_QUOTE_CHAR
            )
        else:
            return ID_QUOTE_CHAR + quote_id(self.id) + ID_QUOTE_CHAR

    def rename(self, id: str) -> "SupportsQualifiedId":
        return PrefixedId(self.namespace, id)

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
                ID_QUOTE_CHAR
                + quote_id(self.namespace)
                + ID_QUOTE_CHAR
                + "."
                + ID_QUOTE_CHAR
                + quote_id(self.id)
                + ID_QUOTE_CHAR
            )
        else:
            return ID_QUOTE_CHAR + quote_id(self.id) + ID_QUOTE_CHAR

    def rename(self, id: str) -> "SupportsQualifiedId":
        return QualifiedId(self.namespace, id)

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

    def rename(self, id: str) -> "SupportsQualifiedId":
        return GlobalId(id)

    def __str__(self) -> str:
        return self.id
