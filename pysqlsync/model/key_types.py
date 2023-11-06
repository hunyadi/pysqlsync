from typing import Annotated, TypeVar

T = TypeVar("T")


class PrimaryKeyTag:
    "Marks a field as the primary key of a table."

    def __repr__(self) -> str:
        return "PrimaryKey"


PrimaryKey = Annotated[T, PrimaryKeyTag()]
