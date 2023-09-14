from typing import Annotated, TypeAlias, TypeVar

T = TypeVar("T")


class PrimaryKeyTag:
    "Marks a field as the primary key of a table."

    def __repr__(self) -> str:
        return "PrimaryKey"


PrimaryKey: TypeAlias = Annotated[T, PrimaryKeyTag()]
