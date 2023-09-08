import unittest

import tests.tables as tables
from pysqlsync.formation.converter import (
    dataclass_to_struct,
    dataclass_to_table,
    module_to_sql,
)
from pysqlsync.formation.object_types import Column, StructMember
from pysqlsync.model.data_types import (
    SqlCharacterType,
    SqlDoubleType,
    SqlIntegerType,
    SqlUuidType,
)
from pysqlsync.model.id_types import LocalId, QualifiedId
from pysqlsync.model.properties import PrimaryKey


class TestConverter(unittest.TestCase):
    def test_primary_key(self) -> None:
        table_def = dataclass_to_table(tables.Address)
        self.assertListEqual(
            [
                Column(LocalId("id"), SqlIntegerType(8), False),
                Column(LocalId("city"), SqlCharacterType(), False),
                Column(LocalId("state"), SqlCharacterType(), True),
            ],
            table_def.columns,
        )

    def test_foreign_key(self) -> None:
        table_def = dataclass_to_table(tables.Person)
        self.assertEqual(table_def.name, QualifiedId(None, "Person"))
        self.assertEqual(table_def.description, "A person.")
        self.assertListEqual(
            table_def.columns,
            [
                Column(LocalId("id"), SqlIntegerType(8), False),
                Column(
                    LocalId("address"),
                    SqlIntegerType(8),
                    False,
                    "The address of the person's permanent residence.",
                ),
            ],
        )

    def test_recursive_table(self) -> None:
        table_def = dataclass_to_table(tables.Employee)
        self.assertListEqual(
            table_def.columns,
            [
                Column(LocalId("id"), SqlUuidType(), False),
                Column(LocalId("name"), SqlCharacterType(), False),
                Column(LocalId("reports_to"), SqlUuidType(), False),
            ],
        )

    def test_module(self) -> None:
        namespace = module_to_sql(tables)
        print(namespace)

    def test_struct(self) -> None:
        struct_def = dataclass_to_struct(tables.Coordinates)
        self.assertEqual(
            struct_def.description, "Coordinates in the geographic coordinate system."
        )
        self.assertListEqual(
            struct_def.members,
            [
                StructMember(LocalId("lat"), SqlDoubleType(), "Latitude in degrees."),
                StructMember(LocalId("long"), SqlDoubleType(), "Longitude in degrees."),
            ],
        )


if __name__ == "__main__":
    unittest.main()
