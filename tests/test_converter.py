import copy
import unittest

import tests.empty as empty
import tests.tables as tables
from pysqlsync.formation.object_types import Column, StructMember
from pysqlsync.formation.py_to_sql import (
    ENUM_NAME_LENGTH,
    DataclassConverter,
    DataclassConverterOptions,
    EnumMode,
    NamespaceMapping,
    StructMode,
    dataclass_to_struct,
    dataclass_to_table,
    module_to_catalog,
)
from pysqlsync.formation.sql_to_py import SqlConverterOptions, table_to_dataclass
from pysqlsync.model.data_types import (
    SqlCharacterType,
    SqlDoubleType,
    SqlIntegerType,
    SqlUserDefinedType,
    SqlUuidType,
)
from pysqlsync.model.id_types import LocalId, QualifiedId
from pysqlsync.python_types import dataclass_to_code


class TestConverter(unittest.TestCase):
    def test_primary_key(self) -> None:
        table_def = dataclass_to_table(tables.Address)
        self.assertListEqual(
            list(table_def.columns.values()),
            [
                Column(LocalId("id"), SqlIntegerType(8), False),
                Column(LocalId("city"), SqlCharacterType(), False),
                Column(LocalId("state"), SqlCharacterType(), True),
            ],
        )

    def test_foreign_key(self) -> None:
        options = DataclassConverterOptions(namespaces=NamespaceMapping({tables: None}))
        table_def = dataclass_to_table(tables.Person, options=options)
        self.assertEqual(table_def.name, QualifiedId(None, "Person"))
        self.assertEqual(table_def.description, "A person.")
        self.assertListEqual(
            list(table_def.columns.values()),
            [
                Column(LocalId("id"), SqlIntegerType(8), False),
                Column(
                    LocalId("address"),
                    SqlIntegerType(8),
                    False,
                    description="The address of the person's permanent residence.",
                ),
            ],
        )

    def test_recursive_table(self) -> None:
        table_def = dataclass_to_table(tables.Employee)
        self.assertListEqual(
            list(table_def.columns.values()),
            [
                Column(LocalId("id"), SqlUuidType(), False),
                Column(LocalId("name"), SqlCharacterType(), False),
                Column(LocalId("reports_to"), SqlUuidType(), False),
            ],
        )

    def test_enum_type(self) -> None:
        options = DataclassConverterOptions(
            enum_mode=EnumMode.TYPE, namespaces=NamespaceMapping({tables: None})
        )
        table_def = dataclass_to_table(tables.EnumTable, options=options)
        self.assertListEqual(
            list(table_def.columns.values()),
            [
                Column(LocalId("id"), SqlIntegerType(8), False),
                Column(
                    LocalId("state"),
                    SqlUserDefinedType(QualifiedId(None, "WorkflowState")),
                    False,
                ),
            ],
        )

    def test_enum_relation(self) -> None:
        options = DataclassConverterOptions(
            enum_mode=EnumMode.RELATION, namespaces=NamespaceMapping({tables: None})
        )
        converter = DataclassConverter(options=options)
        catalog = converter.dataclasses_to_catalog([tables.EnumTable])
        table_def = catalog.get_table(QualifiedId(None, tables.EnumTable.__name__))
        self.assertListEqual(
            list(table_def.columns.values()),
            [
                Column(LocalId("id"), SqlIntegerType(8), False),
                Column(LocalId("state"), SqlIntegerType(4), False),
            ],
        )
        enum_def = catalog.get_table(QualifiedId(None, tables.WorkflowState.__name__))
        self.assertListEqual(
            list(enum_def.columns.values()),
            [
                Column(LocalId("id"), SqlIntegerType(4), False, identity=True),
                Column(LocalId("value"), SqlCharacterType(ENUM_NAME_LENGTH), False),
            ],
        )

    def test_struct_definition(self) -> None:
        struct_def = dataclass_to_struct(tables.Coordinates)
        self.assertEqual(
            struct_def.description, "Coordinates in the geographic coordinate system."
        )
        self.assertListEqual(
            list(struct_def.members.values()),
            [
                StructMember(LocalId("lat"), SqlDoubleType(), "Latitude in degrees."),
                StructMember(LocalId("long"), SqlDoubleType(), "Longitude in degrees."),
            ],
        )

    def test_struct_reference(self) -> None:
        table_def = dataclass_to_table(
            tables.Location,
            options=DataclassConverterOptions(struct_mode=StructMode.TYPE),
        )
        self.assertListEqual(
            list(table_def.columns.values()),
            [
                Column(LocalId("id"), SqlIntegerType(8), False),
                Column(
                    LocalId("coords"),
                    SqlUserDefinedType(
                        QualifiedId(tables.__name__, tables.Coordinates.__name__)
                    ),
                    False,
                ),
            ],
        )

    def test_module(self) -> None:
        catalog = module_to_catalog(
            tables,
            options=DataclassConverterOptions(
                enum_mode=EnumMode.CHECK,
                struct_mode=StructMode.JSON,
                namespaces=NamespaceMapping({tables: "public"}),
            ),
        )
        for table in catalog.namespaces["public"].tables.values():
            cls = table_to_dataclass(table, SqlConverterOptions({"public": empty}))
            str(dataclass_to_code(cls))

    def test_mutate(self) -> None:
        source = module_to_catalog(
            tables,
            options=DataclassConverterOptions(
                enum_mode=EnumMode.TYPE,
                struct_mode=StructMode.TYPE,
                namespaces=NamespaceMapping({tables: "public"}),
            ),
        )
        target = copy.deepcopy(source)
        target_ns = target.namespaces["public"]
        target_ns.enums["WorkflowState"].values.append("unknown")
        target_ns.tables.remove("Employee")
        target_ns.tables["UserTable"].columns.remove("homepage_url")
        target_ns.tables["UserTable"].columns.add(
            Column(LocalId("social_url"), SqlCharacterType(), False)
        )
        self.assertEqual(
            target.mutate_stmt(source),
            'ALTER TYPE "public"."WorkflowState"\n'
            "ADD VALUE 'unknown';\n"
            'ALTER TABLE "public"."UserTable"\n'
            'DROP COLUMN "homepage_url",\n'
            'ADD COLUMN "social_url" text NOT NULL;\n'
            'DROP TABLE "public"."Employee";',
        )


if __name__ == "__main__":
    unittest.main()
