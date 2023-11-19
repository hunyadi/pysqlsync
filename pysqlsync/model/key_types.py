from typing import Annotated, TypeVar, Union

T = TypeVar("T")


class PrimaryKeyTag:
    "Marks a field as the primary key of a table."

    def __repr__(self) -> str:
        return "PrimaryKey"


class IdentityTag:
    "Marks a field as an identity column in a table."

    def __repr__(self) -> str:
        return "Identity"


class UniqueTag:
    "Marks a field as a column with a unique constraint."

    def __repr__(self) -> str:
        return "Unique"


class DefaultTag:
    "The placeholder value DEFAULT."

    def __repr__(self) -> str:
        return "DEFAULT"


DEFAULT = DefaultTag()

PrimaryKey = Annotated[T, PrimaryKeyTag()]
Identity = Annotated[Union[T, DefaultTag], IdentityTag()]
Unique = Annotated[T, UniqueTag()]
