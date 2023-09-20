import enum
import unittest
from dataclasses import dataclass
from typing import Annotated, Optional, TypeAlias, TypeVar

from pysqlsync.python_types import dataclass_to_code, enum_class_to_code


@dataclass(frozen=True)
class TypeTag:
    "A type annotation."


T = TypeVar("T")
TypeWrapper: TypeAlias = Annotated[T, TypeTag()]


@dataclass
class Example:
    """
    Documentation string.

    :param var_int: An integer.
    :param var_str: A string.
    """

    static_var_int = 1
    static_var_str = "string"

    var_bool: bool
    var_int: TypeWrapper[int]
    var_str: str
    var_opt: Optional[str] = None


class WorkflowState(enum.Enum):
    "An enumeration type."

    active = "active"
    inactive = "inactive"
    deleted = "deleted"


class TestTypes(unittest.TestCase):
    def test_dataclass(self) -> None:
        self.assertMultiLineEqual(
            dataclass_to_code(Example),
            "@dataclass\n"
            "class Example:\n"
            '    """\n'
            "    Documentation string.\n"
            "\n"
            "    :param var_int: An integer.\n"
            "    :param var_str: A string.\n"
            '    """\n'
            "\n"
            "    static_var_int = 1\n"
            "    static_var_str = 'string'\n"
            "\n"
            "    var_bool: bool\n"
            "    var_int: Annotated[int, TypeTag()]\n"
            "    var_str: str\n"
            "    var_opt: Optional[str] = None\n",
        )

    def test_enum(self) -> None:
        self.assertMultiLineEqual(
            enum_class_to_code(WorkflowState),
            "@enum.unique\n"
            "class WorkflowState(enum.Enum):\n"
            "    'An enumeration type.'\n"
            "\n"
            "    active = 'active'\n"
            "    inactive = 'inactive'\n"
            "    deleted = 'deleted'\n",
        )


if __name__ == "__main__":
    unittest.main()
