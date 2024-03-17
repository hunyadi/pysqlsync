import enum
from dataclasses import dataclass
from typing import Optional

from pysqlsync.model.key_types import PrimaryKey


@dataclass
class Country:
    "A complex type capturing the core properties of a country."

    iso_code: str
    name: str


@enum.unique
class CountryEnum(enum.Enum):
    at = Country("at", "Austria")
    be = Country("be", "Belgium")
    bg = Country("bg", "Bulgaria")
    cz = Country("cz", "Czechia")
    de = Country("de", "Germany")
    dk = Country("dk", "Denmark")
    ee = Country("ee", "Estonia")
    es = Country("es", "Spain")
    fi = Country("fi", "Finland")
    fr = Country("fr", "France")
    gr = Country("gr", "Greece")
    hr = Country("hr", "Croatia")
    hu = Country("hu", "Hungary")
    ie = Country("ie", "Ireland")
    it = Country("it", "Italy")
    lt = Country("lt", "Lithuania")
    lu = Country("lu", "Luxembourg")
    lv = Country("lv", "Latvia")
    nl = Country("nl", "Netherlands")
    pl = Country("pl", "Poland")
    pt = Country("pt", "Portugal")
    ro = Country("ro", "Romania")
    se = Country("se", "Sweden")
    si = Country("si", "Slovenia")
    sk = Country("sk", "Slovakia")
    us = Country("us", "United States")


@dataclass
class DataclassEnumTable:
    id: PrimaryKey[int]
    country: CountryEnum
    optional_country: Optional[CountryEnum]
