import copy
import dataclasses
import re
from typing import Optional

from ..model.data_types import (
    SqlBooleanType,
    SqlDataType,
    SqlDateType,
    SqlDecimalType,
    SqlDoubleType,
    SqlEnumType,
    SqlFixedBinaryType,
    SqlFixedCharacterType,
    SqlFloatType,
    SqlIntegerType,
    SqlJsonType,
    SqlRealType,
    SqlTimestampType,
    SqlTimeType,
    SqlUserDefinedType,
    SqlUuidType,
    SqlVariableBinaryType,
    SqlVariableCharacterType,
)
from ..model.id_types import QualifiedId


@dataclasses.dataclass
class SqlDiscoveryOptions:
    substitutions: dict[str, SqlDataType] = dataclasses.field(default_factory=dict)


class SqlDiscovery:
    options: SqlDiscoveryOptions

    def __init__(self, options: Optional[SqlDiscoveryOptions] = None) -> None:
        self.options = options if options is not None else SqlDiscoveryOptions()

    def sql_data_type_from_name(
        self,
        type_name: str,
        type_schema: Optional[str] = None,
    ) -> Optional[SqlDataType]:
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
        elif type_name in ["numeric", "decimal"]:
            return SqlDecimalType()
        elif type_name in ["real", "float4"]:
            return SqlRealType()
        elif type_name in ["double", "double precision", "float8"]:
            return SqlDoubleType()
        elif type_name == "float":
            return SqlFloatType()
        elif type_name in ["timestamp", "timestamp without time zone"]:
            return SqlTimestampType(None, False)
        elif type_name == "timestamp with time zone":
            return SqlTimestampType(None, True)
        elif type_name == "datetime":
            return SqlTimestampType()
        elif type_name == "date":
            return SqlDateType()
        elif type_name in ["time", "time without time zone"]:
            return SqlTimeType(None, False)
        elif type_name == "time with time zone":
            return SqlTimeType(None, True)
        elif type_name in ["char", "character"]:
            return SqlFixedCharacterType()
        elif type_name in ["varchar", "character varying", "text"]:
            return SqlVariableCharacterType()
        elif type_name == "binary":
            return SqlFixedBinaryType()
        elif type_name in ["varbinary", "binary varying", "bytea"]:
            return SqlVariableBinaryType()
        elif type_name in ["json", "jsonb"]:  # PostgreSQL-specific
            return SqlJsonType()
        elif type_name == "uuid":  # PostgreSQL-specific
            return SqlUuidType()

        if type_schema is not None:
            return SqlUserDefinedType(QualifiedId(type_schema, type_name))

        return None

    def sql_data_type_from_def(self, type_def: str) -> Optional[SqlDataType]:
        m = re.fullmatch(
            r"^(?:decimal|numeric)[(](\d+),\s*(\d+)[)]$", type_def, re.IGNORECASE
        )
        if m is not None:
            return SqlDecimalType(int(m.group(1)), int(m.group(2)))

        m = re.fullmatch(
            r"^timestamp[(](\d+)[)]\s*(with(?:out)? time zone)?$",
            type_def,
            re.IGNORECASE,
        )
        if m is not None:
            if m.group(2) == "with time zone":
                return SqlTimestampType(int(m.group(1)), True)
            else:
                return SqlTimestampType(int(m.group(1)), False)

        m = re.fullmatch(r"^char(?:acter)?[(](\d+)[)]$", type_def, re.IGNORECASE)
        if m is not None:
            return SqlFixedCharacterType(int(m.group(1)))

        m = re.fullmatch(
            r"^(?:varchar|character varying)[(](\d+)[)]$", type_def, re.IGNORECASE
        )
        if m is not None:
            return SqlVariableCharacterType(int(m.group(1)))

        m = re.fullmatch(r"^binary[(](\d+)[)]$", type_def, re.IGNORECASE)
        if m is not None:
            return SqlFixedBinaryType(int(m.group(1)))

        m = re.fullmatch(
            r"^(?:varbinary|binary varying)[(](\d+)[)]$", type_def, re.IGNORECASE
        )
        if m is not None:
            return SqlVariableBinaryType(int(m.group(1)))

        # MySQL and Oracle
        m = re.fullmatch(r"^enum[(](.+)[)]$", type_def, re.IGNORECASE)
        if m is not None:
            value_list = m.group(1)
            values = [
                value for value in re.findall(r"'((?:[^']+|'')*)'(?:,|$)", value_list)
            ]
            return SqlEnumType(values)

        return None

    def sql_data_type_from_spec(
        self,
        *,
        type_name: str,
        type_schema: Optional[str] = None,
        type_def: Optional[str] = None,
        character_maximum_length: Optional[int] = None,
        numeric_precision: Optional[int] = None,
        numeric_scale: Optional[int] = None,
        datetime_precision: Optional[int] = None,
    ) -> SqlDataType:
        "Determines the column type from SQL column attribute data extracted from the information schema table."

        sql_type: Optional[SqlDataType] = None
        if type_schema is None or type_schema == "pg_catalog":
            sql_type = copy.copy(self.options.substitutions.get(type_name))

        if sql_type is None and type_def is not None:
            sql_type = self.sql_data_type_from_def(type_def)

        if sql_type is None:
            sql_type = self.sql_data_type_from_name(type_name, type_schema)

        if sql_type is None:
            raise TypeError(f"unrecognized SQL type: {type_name}")

        if isinstance(sql_type, SqlDecimalType):
            if numeric_precision is not None:
                sql_type.precision = numeric_precision  # precision in base 10
            if numeric_scale is not None:
                sql_type.scale = numeric_scale
        elif isinstance(sql_type, SqlFloatType):
            if numeric_precision is not None:
                sql_type.precision = numeric_precision  # precision in base 2
        elif isinstance(sql_type, SqlTimestampType):
            if datetime_precision is not None and datetime_precision > 0:
                sql_type.precision = datetime_precision
        elif isinstance(sql_type, SqlTimeType):
            if datetime_precision is not None and datetime_precision > 0:
                sql_type.precision = datetime_precision
        elif isinstance(sql_type, SqlFixedCharacterType):
            if character_maximum_length is not None:
                sql_type.limit = character_maximum_length
        elif isinstance(sql_type, SqlVariableCharacterType):
            if character_maximum_length is not None and character_maximum_length > 0:
                sql_type.limit = character_maximum_length
        elif isinstance(sql_type, SqlFixedBinaryType):
            if character_maximum_length is not None:
                sql_type.storage = character_maximum_length
        elif isinstance(sql_type, SqlVariableBinaryType):
            if character_maximum_length is not None and character_maximum_length > 0:
                sql_type.storage = character_maximum_length

        return sql_type
