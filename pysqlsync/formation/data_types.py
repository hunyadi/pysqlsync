import dataclasses
import re

from ..model.data_types import *
from ..model.id_types import QualifiedId


@dataclass
class SqlDiscoveryOptions:
    substitutions: dict[str, SqlDataType] = dataclasses.field(default_factory=dict)


class SqlDiscovery:
    options: SqlDiscoveryOptions

    def __init__(self, options: Optional[SqlDiscoveryOptions] = None) -> None:
        self.options = options if options is not None else SqlDiscoveryOptions()

    def sql_data_type_from_spec(
        self,
        type_name: str,
        type_schema: Optional[str] = None,
        *,
        character_maximum_length: Optional[int] = None,
        numeric_precision: Optional[int] = None,
        numeric_scale: Optional[int] = None,
        datetime_precision: Optional[int] = None,
    ) -> SqlDataType:
        "Determines the column type from SQL column attribute data extracted from the information schema table."

        if type_schema is None:
            substitute = self.options.substitutions.get(type_name)
            if substitute is not None:
                return substitute

        if type_name in ["bool", "boolean"]:
            return SqlBooleanType()
        elif type_name in ["tinyint", "tinyint(1)", "int1"]:
            return SqlIntegerType(1)
        elif type_name in ["smallint", "int2"]:
            return SqlIntegerType(2)
        elif type_name in ["integer", "int", "int4"]:
            return SqlIntegerType(4)
        elif type_name in ["bigint", "int8"]:
            return SqlIntegerType(8)
        elif type_name == "numeric" or type_name == "decimal":
            return SqlDecimalType(
                numeric_precision, numeric_scale
            )  # precision in base 10
        elif type_name == "real":
            if numeric_precision is None or numeric_precision == 24:
                return SqlRealType()
            else:
                return SqlFloatType(numeric_precision)  # precision in base 2
        elif type_name == "double" or type_name == "double precision":
            if numeric_precision is None or numeric_precision == 53:
                return SqlDoubleType()
            else:
                return SqlFloatType(numeric_precision)  # precision in base 2
        elif type_name == "float":
            return SqlFloatType(numeric_precision)  # precision in base 2
        elif type_name == "float4":
            return SqlRealType()
        elif type_name == "float8":
            return SqlDoubleType()
        elif type_name == "timestamp" or type_name == "timestamp without time zone":
            return SqlTimestampType(datetime_precision, False)
        elif type_name == "timestamp with time zone":
            return SqlTimestampType(datetime_precision, True)
        elif type_name == "datetime":  # MySQL-specific
            return SqlTimestampType()
        elif type_name == "date":
            return SqlDateType()
        elif type_name == "time" or type_name == "time without time zone":
            return SqlTimeType(datetime_precision, False)
        elif type_name == "time with time zone":
            return SqlTimeType(datetime_precision, True)
        elif type_name == "varchar" or type_name == "character varying":
            return SqlCharacterType(limit=character_maximum_length)
        elif type_name == "text":
            return SqlCharacterType()
        elif type_name == "mediumtext":  # MySQL-specific
            return SqlCharacterType(storage=16777215)
        elif type_name == "longtext":  # MySQL-specific
            return SqlCharacterType(storage=4294967295)
        elif type_name == "blob":  # MySQL-specific
            return SqlVariableBinaryType(storage=65535)
        elif type_name == "mediumblob":  # MySQL-specific
            return SqlVariableBinaryType(storage=16777215)
        elif type_name == "longblob":  # MySQL-specific
            return SqlVariableBinaryType(storage=4294967295)
        elif type_name == "bytea":  # PostgreSQL-specific
            return SqlVariableBinaryType()
        elif type_name == "uuid":  # PostgreSQL-specific
            return SqlUuidType()
        elif type_name == "json" or type_name == "jsonb":
            return SqlJsonType()

        m = re.fullmatch(
            r"^(?:decimal|numeric)[(](\d+),\s*(\d+)[)]$", type_name, re.IGNORECASE
        )
        if m is not None:
            return SqlDecimalType(int(m.group(1)), int(m.group(2)))

        m = re.fullmatch(r"^timestamp[(](\d+)[)]$", type_name, re.IGNORECASE)
        if m is not None:
            return SqlTimestampType(int(m.group(1)), False)

        m = re.fullmatch(r"^varchar[(](\d+)[)]$", type_name, re.IGNORECASE)
        if m is not None:
            return SqlCharacterType(int(m.group(1)))

        m = re.fullmatch(r"^binary[(](\d+)[)]$", type_name, re.IGNORECASE)
        if m is not None:
            return SqlFixedBinaryType(int(m.group(1)))

        m = re.fullmatch(r"^varbinary[(](\d+)[)]$", type_name, re.IGNORECASE)
        if m is not None:
            return SqlVariableBinaryType(int(m.group(1)))

        m = re.fullmatch(r"^enum[(](.+)[)]$", type_name, re.IGNORECASE)
        if m is not None:
            value_list = m.group(1)
            values = [
                value for value in re.findall(r"'((?:[^']+|'')*)'(?:,|$)", value_list)
            ]
            return SqlEnumType(values)

        if type_schema is not None:
            return SqlUserDefinedType(QualifiedId(type_schema, type_name))

        raise TypeError(f"unrecognized SQL type: {type_name}")
