import copy
import unittest

from strong_typing.inspection import create_module

from pysqlsync.formation.mutation import Mutator, MutatorOptions
from pysqlsync.formation.object_types import (
    Column,
    ConstraintReference,
    ForeignConstraint,
    StructMember,
    UniqueConstraint,
)
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
    SqlDoubleType,
    SqlFixedCharacterType,
    SqlIntegerType,
    SqlUserDefinedType,
    SqlUuidType,
    SqlVariableCharacterType,
)
from pysqlsync.model.id_types import LocalId, QualifiedId
from pysqlsync.python_types import dataclass_to_code, module_to_code
from tests import tables
from tests.model import country, user


class TestConverter(unittest.TestCase):
    @property
    def options(self) -> DataclassConverterOptions:
        return DataclassConverterOptions(namespaces=NamespaceMapping({tables: None}))

    def test_primary_key(self) -> None:
        table_def = dataclass_to_table(tables.Address, options=self.options)
        self.assertListEqual(
            list(table_def.columns.values()),
            [
                Column.create(LocalId("id"), SqlIntegerType(8), nullable=False),
                Column.create(LocalId("city"), SqlVariableCharacterType(), nullable=False),
                Column.create(LocalId("state"), SqlVariableCharacterType(), nullable=True),
            ],
        )

    def test_default(self) -> None:
        table_def = dataclass_to_table(tables.DefaultNumericTable, options=self.options)
        self.assertListEqual(
            list(table_def.columns.values()),
            [
                Column.create(LocalId("id"), SqlIntegerType(8), nullable=False),
                Column.create(
                    LocalId("integer_8"),
                    SqlIntegerType(2),
                    nullable=False,
                    default="127",
                ),
                Column.create(
                    LocalId("integer_16"),
                    SqlIntegerType(2),
                    nullable=False,
                    default="32767",
                ),
                Column.create(
                    LocalId("integer_32"),
                    SqlIntegerType(4),
                    nullable=False,
                    default="2147483647",
                ),
                Column.create(
                    LocalId("integer_64"),
                    SqlIntegerType(8),
                    nullable=False,
                    default="0",
                ),
                Column.create(
                    LocalId("integer"),
                    SqlIntegerType(8),
                    nullable=False,
                    default="23",
                ),
            ],
        )

    def test_identity(self) -> None:
        table_def = dataclass_to_table(tables.UniqueTable, options=self.options)
        self.assertListEqual(
            list(table_def.columns.values()),
            [
                Column.create(LocalId("id"), SqlIntegerType(8), nullable=False, identity=True),
                Column.create(LocalId("unique"), SqlVariableCharacterType(64), nullable=False),
            ],
        )
        self.assertListEqual(
            list(table_def.constraints.values()),
            [UniqueConstraint(LocalId("uq_UniqueTable_unique"), (LocalId("unique"),))],
        )

    def test_foreign_key(self) -> None:
        table_def = dataclass_to_table(tables.Person, options=self.options)
        self.assertEqual(table_def.name, QualifiedId(None, "Person"))
        self.assertEqual(table_def.description, "A person.")
        self.assertListEqual(
            list(table_def.columns.values()),
            [
                Column.create(LocalId("id"), SqlIntegerType(8), nullable=False),
                Column.create(
                    LocalId("name"),
                    SqlVariableCharacterType(),
                    nullable=False,
                    description="The person's full name.",
                ),
                Column.create(
                    LocalId("address"),
                    SqlIntegerType(8),
                    nullable=False,
                    description="The address of the person's permanent residence.",
                ),
            ],
        )

    def test_recursive_table(self) -> None:
        table_def = dataclass_to_table(tables.Employee, options=self.options)
        self.assertListEqual(
            list(table_def.columns.values()),
            [
                Column.create(LocalId("id"), SqlUuidType(), nullable=False),
                Column.create(LocalId("name"), SqlVariableCharacterType(), nullable=False),
                Column.create(LocalId("reports_to"), SqlUuidType(), nullable=False),
            ],
        )

    def test_enum_type(self) -> None:
        options = DataclassConverterOptions(enum_mode=EnumMode.TYPE, namespaces=NamespaceMapping({tables: None}))
        table_def = dataclass_to_table(tables.EnumTable, options=options)
        self.assertListEqual(
            list(table_def.columns.values()),
            [
                Column.create(LocalId("id"), SqlIntegerType(8), nullable=False),
                Column.create(
                    LocalId("state"),
                    SqlUserDefinedType(QualifiedId(None, "WorkflowState")),
                    nullable=False,
                ),
                Column.create(
                    LocalId("optional_state"),
                    SqlUserDefinedType(QualifiedId(None, "WorkflowState")),
                    nullable=True,
                ),
            ],
        )

    def test_extensible_enum(self) -> None:
        options = DataclassConverterOptions(enum_mode=EnumMode.TYPE, namespaces=NamespaceMapping({tables: None}))
        converter = DataclassConverter(options=options)
        catalog = converter.dataclasses_to_catalog([tables.ExtensibleEnumTable])
        table_name = tables.ExtensibleEnumTable.__name__
        table_def = catalog.get_table(QualifiedId(None, table_name))
        self.assertListEqual(
            list(table_def.columns.values()),
            [
                Column.create(LocalId("id"), SqlIntegerType(8), nullable=False),
                Column.create(LocalId("state"), SqlIntegerType(4), nullable=False),
                Column.create(LocalId("optional_state"), SqlIntegerType(4), nullable=True),
            ],
        )
        enum_name = tables.ExtensibleEnum.__name__
        enum_def = catalog.get_table(QualifiedId(None, enum_name))
        self.assertListEqual(
            list(enum_def.columns.values()),
            [
                Column.create(LocalId("id"), SqlIntegerType(4), nullable=False, identity=True),
                Column.create(
                    LocalId("value"),
                    SqlVariableCharacterType(ENUM_NAME_LENGTH),
                    nullable=False,
                ),
            ],
        )
        self.assertListEqual(
            list(table_def.constraints.values()),
            [
                ForeignConstraint(
                    name=LocalId(id=f"fk_{table_name}_state"),
                    foreign_columns=(LocalId(id="state"),),
                    reference=ConstraintReference(
                        table=QualifiedId(namespace=None, id=enum_name),
                        columns=(LocalId(id="id"),),
                    ),
                ),
                ForeignConstraint(
                    name=LocalId(id=f"fk_{table_name}_optional_state"),
                    foreign_columns=(LocalId(id="optional_state"),),
                    reference=ConstraintReference(
                        table=QualifiedId(namespace=None, id=enum_name),
                        columns=(LocalId(id="id"),),
                    ),
                ),
            ],
        )

    def test_enum_relation(self) -> None:
        options = DataclassConverterOptions(
            enum_mode=EnumMode.RELATION,
            namespaces=NamespaceMapping({tables: None}),
        )
        converter = DataclassConverter(options=options)
        catalog = converter.dataclasses_to_catalog([tables.EnumTable])
        table_name = tables.EnumTable.__name__
        table_def = catalog.get_table(QualifiedId(None, table_name))
        self.assertListEqual(
            list(table_def.columns.values()),
            [
                Column.create(LocalId("id"), SqlIntegerType(8), nullable=False),
                Column.create(LocalId("state"), SqlIntegerType(4), nullable=False),
                Column.create(LocalId("optional_state"), SqlIntegerType(4), nullable=True),
            ],
        )
        enum_name = tables.WorkflowState.__name__
        enum_def = catalog.get_table(QualifiedId(None, enum_name))
        self.assertListEqual(
            list(enum_def.columns.values()),
            [
                Column.create(LocalId("id"), SqlIntegerType(4), nullable=False, identity=True),
                Column.create(
                    LocalId("value"),
                    SqlVariableCharacterType(ENUM_NAME_LENGTH),
                    nullable=False,
                ),
            ],
        )
        self.assertListEqual(
            list(table_def.constraints.values()),
            [
                ForeignConstraint(
                    name=LocalId(id=f"fk_{table_name}_state"),
                    foreign_columns=(LocalId(id="state"),),
                    reference=ConstraintReference(
                        table=QualifiedId(namespace=None, id=enum_name),
                        columns=(LocalId(id="id"),),
                    ),
                ),
                ForeignConstraint(
                    name=LocalId(id=f"fk_{table_name}_optional_state"),
                    foreign_columns=(LocalId(id="optional_state"),),
                    reference=ConstraintReference(
                        table=QualifiedId(namespace=None, id=enum_name),
                        columns=(LocalId(id="id"),),
                    ),
                ),
            ],
        )

    def test_dataclass_enum_relation(self) -> None:
        options = DataclassConverterOptions(
            enum_mode=EnumMode.RELATION,
            namespaces=NamespaceMapping({country: None}),
        )
        converter = DataclassConverter(options=options)
        catalog = converter.dataclasses_to_catalog([country.DataclassEnumTable])
        table_name = country.DataclassEnumTable.__name__
        table_def = catalog.get_table(QualifiedId(None, table_name))
        self.assertListEqual(
            list(table_def.columns.values()),
            [
                Column.create(LocalId("id"), SqlIntegerType(8), nullable=False),
                Column.create(LocalId("country"), SqlIntegerType(4), nullable=False),
                Column.create(LocalId("optional_country"), SqlIntegerType(4), nullable=True),
            ],
        )
        enum_name = country.CountryEnum.__name__
        enum_def = catalog.get_table(QualifiedId(None, enum_name))
        self.assertListEqual(
            list(enum_def.columns.values()),
            [
                Column.create(
                    LocalId("id"),
                    SqlIntegerType(4),
                    nullable=False,
                    identity=True,
                ),
                Column.create(
                    LocalId("iso_code"),
                    SqlVariableCharacterType(),
                    nullable=False,
                ),
                Column.create(
                    LocalId("name"),
                    SqlVariableCharacterType(),
                    nullable=False,
                ),
            ],
        )
        self.assertListEqual(
            list(table_def.constraints.values()),
            [
                ForeignConstraint(
                    name=LocalId(id=f"fk_{table_name}_country"),
                    foreign_columns=(LocalId(id="country"),),
                    reference=ConstraintReference(
                        table=QualifiedId(namespace=None, id=enum_name),
                        columns=(LocalId(id="id"),),
                    ),
                ),
                ForeignConstraint(
                    name=LocalId(id=f"fk_{table_name}_optional_country"),
                    foreign_columns=(LocalId(id="optional_country"),),
                    reference=ConstraintReference(
                        table=QualifiedId(namespace=None, id=enum_name),
                        columns=(LocalId(id="id"),),
                    ),
                ),
            ],
        )

    def test_literal_type(self) -> None:
        table_def = dataclass_to_table(tables.LiteralTable, options=self.options)
        self.assertListEqual(
            list(table_def.columns.values()),
            [
                Column.create(LocalId("id"), SqlIntegerType(8), nullable=False),
                Column.create(
                    LocalId("single"),
                    SqlFixedCharacterType(limit=5),
                    nullable=False,
                ),
                Column.create(
                    LocalId("multiple"),
                    SqlFixedCharacterType(limit=4),
                    nullable=False,
                ),
                Column.create(
                    LocalId("union"),
                    SqlVariableCharacterType(limit=255),
                    nullable=False,
                ),
                Column.create(
                    LocalId("unbounded"),
                    SqlVariableCharacterType(),
                    nullable=False,
                ),
            ],
        )

    def test_struct_definition(self) -> None:
        struct_def = dataclass_to_struct(tables.Coordinates, options=self.options)
        self.assertEqual(struct_def.description, "Coordinates in the geographic coordinate system.")
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
            options=DataclassConverterOptions(
                struct_mode=StructMode.TYPE,
                namespaces=NamespaceMapping({tables: "sample"}),
            ),
        )
        self.assertListEqual(
            list(table_def.columns.values()),
            [
                Column.create(LocalId("id"), SqlIntegerType(8), nullable=False),
                Column.create(
                    LocalId("coords"),
                    SqlUserDefinedType(QualifiedId("sample", tables.Coordinates.__name__)),
                    nullable=False,
                ),
            ],
        )

    @unittest.skip("create_module")
    def test_module(self) -> None:
        catalog = module_to_catalog(
            tables,
            options=DataclassConverterOptions(
                enum_mode=EnumMode.CHECK,
                struct_mode=StructMode.JSON,
                namespaces=NamespaceMapping({tables: "public"}),
            ),
        )

        module = create_module(f"{self.__module__}.empty")
        for table in catalog.namespaces["public"].tables.values():
            cls = table_to_dataclass(table, SqlConverterOptions({"public": module}))
            self.assertTrue(dataclass_to_code(cls))

        self.assertTrue(module_to_code(module))

    def test_mutate(self) -> None:
        self.maxDiff = None
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
        target_ns.enums["WorkflowState"].values.append("value_1")
        target_ns.enums["WorkflowState"].values.append("value_2")
        target_ns.structs.remove("Coordinates")
        target_ns.tables.remove("Employee")
        self.assertEqual(
            Mutator().mutate_catalog_stmt(source, target),
            """ALTER TYPE "public"."WorkflowState" ADD VALUE 'value_1';\n"""
            """ALTER TYPE "public"."WorkflowState" ADD VALUE 'value_2';\n"""
            'DROP TABLE "public"."Employee";\n'
            'DROP TYPE "public"."Coordinates";',
        )
        self.assertEqual(
            Mutator(
                MutatorOptions(
                    allow_drop_enum=False,
                    allow_drop_struct=False,
                    allow_drop_table=False,
                    allow_drop_namespace=False,
                )
            ).mutate_catalog_stmt(source, target),
            """ALTER TYPE "public"."WorkflowState" ADD VALUE 'value_1';\n"""
            """ALTER TYPE "public"."WorkflowState" ADD VALUE 'value_2';""",
        )

        source = module_to_catalog(
            user,
            options=DataclassConverterOptions(
                enum_mode=EnumMode.TYPE,
                struct_mode=StructMode.TYPE,
                namespaces=NamespaceMapping({user: "public"}),
            ),
        )
        target = copy.deepcopy(source)
        target_ns = target.namespaces["public"]
        user_table = target_ns.tables["UserTable"]
        user_table.columns["created_at"].default = "CURRENT_TIMESTAMP()"
        user_table.columns.remove("deleted_at")
        user_table.columns["short_name"].nullable = True
        user_table.columns["homepage_url"].default = "'https://example.com/'"
        user_table.columns["homepage_url"].nullable = False
        user_table.columns.add(
            Column.create(
                LocalId("social_url"),
                SqlVariableCharacterType(),
                nullable=False,
                default="'https://community.canvaslms.com/'",
            )
        )
        self.assertEqual(
            Mutator().mutate_catalog_stmt(source, target),
            'ALTER TABLE "public"."UserTable"\n'
            """ADD COLUMN "social_url" text NOT NULL DEFAULT 'https://community.canvaslms.com/';\n"""
            'ALTER TABLE "public"."UserTable"\n'
            'ALTER COLUMN "created_at" SET DEFAULT CURRENT_TIMESTAMP(),\n'
            'ALTER COLUMN "short_name" DROP NOT NULL,\n'
            'ALTER COLUMN "homepage_url" SET NOT NULL,\n'
            """ALTER COLUMN "homepage_url" SET DEFAULT 'https://example.com/',\n"""
            'DROP COLUMN "deleted_at";',
        )


if __name__ == "__main__":
    unittest.main()
