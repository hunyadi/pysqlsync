from dataclasses import dataclass
from typing import Optional


@dataclass
class LocalId:
    name: str

    def __str__(self) -> str:
        "Quotes an identifier to be embedded in an SQL statement."
        return '"' + str(self.name).replace('"', '""') + '"'


@dataclass
class QualifiedId:
    namespace: Optional[str]
    name: str

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
