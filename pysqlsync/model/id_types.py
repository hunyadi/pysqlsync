from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class LocalId:
    name: str

    @property
    def compact_id(self) -> str:
        return self.name

    def __str__(self) -> str:
        "Quotes an identifier to be embedded in an SQL statement."
        return '"' + str(self.name).replace('"', '""') + '"'


@dataclass(frozen=True)
class QualifiedId:
    namespace: Optional[str]
    name: str

    @property
    def compact_id(self) -> str:
        return f"{self.namespace}.{self.name}"

    def __str__(self) -> str:
        if self.namespace is not None:
            return (
                '"'
                + str(self.namespace).replace('"', '""')
                + '"."'
                + str(self.name).replace('"', '""')
                + '"'
            )
        else:
            return '"' + str(self.name).replace('"', '""') + '"'
