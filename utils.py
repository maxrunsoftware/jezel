#  Copyright (C) 2022 Max Run Software (dev@maxrunsoftware.com)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

import base64
import colorsys
import dataclasses
import hashlib
import inspect
import json as jsn
import logging
import random
import sys
import threading
import time
from abc import ABC, ABCMeta, abstractmethod
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from itertools import chain, combinations
from types import NoneType, UnionType
from typing import Any, Callable, Dict, Generator, Generic, get_args, get_origin, Iterable, List, Mapping, MutableMapping, NamedTuple, Sequence, Set, Tuple, TypeVar, Union
from uuid import UUID

from ruamel.yaml import YAML

_MODULE_NAME = __name__


class NoArg(Enum):
    NO_ARG = 0


NO_ARG = NoArg.NO_ARG

T = TypeVar('T')


def trim(s: str | Iterable[str] | None, exclude_none=True) -> str | List[str | None] | List[str] | None:
    if s is None: return None
    if isinstance(s, str):
        s = s.strip()
        return None if len(s) == 0 else s

    # iterable
    result: List[str | None] = []
    for item in s:
        if item is None:
            if not exclude_none:
                result.append(item)
        else:
            item = item.strip()
            if len(item) == 0:
                if not exclude_none:
                    result.append(None)
            else:
                result.append(item)
    return result


def trim_casefold(s: str | None) -> str | None: return s if s is None else trim(s.casefold())


def coalesce(*args: T) -> T | None:
    if args is None: return None
    for item in args:
        if item is not None: return item
    return None


def xstr(s):
    if s is None:
        return ""
    else:
        return str(s)


def str2camel(snake_str: str):
    # https://stackoverflow.com/a/19053800
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])


def str2snake(camel_str: str):
    result = ""
    for i, c in enumerate(camel_str):
        if c.isupper() and i != 0:
            result += "_"
            result += c.lower()
        else:
            result += c.lower()
    return result


def url_join(*args: Any | None) -> str | None:
    """
    https://stackoverflow.com/a/11326230
    """
    if args is None: return None
    args_new = ""
    for arg in args:
        arg = trim(xstr(arg))
        if arg is None: continue
        arg = trim(arg.rstrip('/'))
        if arg is None: continue
        if len(args_new) > 0:
            args_new += "/"
        args_new += arg
    return trim(args_new)


def chunks(lst: List[T], items_per_chunk: int) -> Generator[List[T]]:
    """
    https://stackoverflow.com/a/312464
    Yield successive n-sized chunks from lst.
    """
    for i in range(0, len(lst), items_per_chunk):
        yield lst[i:i + items_per_chunk]


def int_commas(value: int) -> str:
    return "{:,}".format(value)


def int_prefix(value: int, length: int) -> str:
    return str(value).zfill(length)


def _first(values: Iterable[T], predicate: Callable[[T], bool], allow_empty: bool) -> T | None:
    if values is None: return None
    if predicate is None:
        for value in values:
            return value
    else:
        for value in values:
            if predicate(value):
                return value
    if allow_empty:
        return None
    else:
        raise ValueError("No items in iterable")


def first_or_none(values: Iterable[T], predicate: Callable[[T], bool] = None) -> T | None:
    return _first(values=values, predicate=predicate, allow_empty=True)


def first(values: Iterable[T], predicate: Callable[[T], bool] = None) -> T:
    return _first(values=values, predicate=predicate, allow_empty=False)


# region binary

def bools2int(bools: List[bool]) -> int:
    """
    Returns binary conversion of list of bools to integer
    https://stackoverflow.com/a/27165675
    """
    if bools is None or len(bools) == 0: return 0
    return sum(2 ** i for i, v in enumerate(reversed(bools)) if v)


def int2bools(n, total_length: int):
    """
    Converts an int value to a list of bools
    https://stackoverflow.com/a/10322018
    """

    lst = [True if digit == '1' else False for digit in bin(n)[2:]]
    return [False] * (total_length - len(lst)) + lst


# endregion binary

class Object:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


# region parse


_BOOL_TRUE: Set[str] = {'true', '1', 't', 'y', 'yes'}
_BOOL_FALSE: Set[str] = {'false', '0', 'f', 'n', 'no'}


def bool_parse(value: Any) -> bool:
    if value is None: raise TypeError("bool_parse() argument must be a string or a bytes-like object, not 'NoneType'")
    if isinstance(value, bool): return value
    v = trim(str(value))
    if v is not None:
        v = v.lower()
        if v in _BOOL_TRUE: return True
        if v in _BOOL_FALSE: return False
    raise ValueError("invalid literal for bool_parse(): " + value.__repr__())


def bool_parse_none(value: Any | None) -> bool | None:
    if value is None: return None
    if isinstance(value, bool): return value
    value = trim(str(value))
    if value is None: return None
    return bool_parse(value)


def int_parse_none(value: Any | None) -> int | None:
    if value is None: return None
    if isinstance(value, int): return value
    value = trim(str(value))
    if value is None: return None
    return int(value)


def float_parse_none(value: Any | None) -> float | None:
    if value is None: return None
    value = trim(str(value))
    if value is None: return None
    return float(value)


def datetime_parse_none(value: Any | None, tz: timezone = None) -> datetime | None:
    if value is None: return None
    value = trim(str(value))
    if value is None: return None
    dt = datetime.fromisoformat(value)
    if tz is not None:
        dt = dt.replace(tzinfo=tz)
    return dt


def uuid_parse_none(value: Any | None) -> UUID | None:
    if value is None: return None
    value = trim(str(value))
    if value is None: return None
    return UUID(value)


# endregion parse


# region Random


def random_datetime(min: datetime | None = None, max: datetime | None = None) -> datetime:
    """ Generates a random datetime between min and max. https://stackoverflow.com/a/553448 """
    if min is None:
        min = datetime.strptime('1/1/1900 12:00 AM', '%m/%d/%Y %I:%M %p')
    if max is None:
        max = datetime.strptime('1/1/2100 12:00 AM', '%m/%d/%Y %I:%M %p')
    delta = max - min
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return min + timedelta(seconds=random_second)


def random_int(min: int | None = 0, max: int | None = 1000) -> int:
    return random.randint(min, max)


def random_bool(percent_true: int = 50) -> bool:
    if percent_true <= 0: return False
    if percent_true >= 100: return True
    return random.randint(1, 100) <= percent_true


def random_pick(items: Iterable):
    if isinstance(items, type):
        if issubclass(items, Enum):
            items = list(items)
    if not isinstance(items, Sequence):
        items = [item for item in items]

    if len(items) == 0:
        return None

    return random.choice(items)


def random_picks(items, count: int) -> List:
    if count < 1: return []
    return [random_pick(items) for _ in range(0, count)]


def random_picks_unique(items, count: int) -> Set:
    if isinstance(items, type):
        if issubclass(items, Enum):
            items = list(items)

    if count < 1: return set()
    if count >= len(items): return set(items)
    if count == 1: return {random_pick(items)}
    items_list = list(items)
    random.shuffle(items_list)
    return set(items_list[:count])


# @formatter:off
_RANDOM_WORDS_NAME_POOL: str = "Roshan Molina Liliana Alston Emmanuel Ward Emmett Gallagher Fariha Read Mathilda Couch Sheikh Plummer Tania Mcleod Ailish David Kien Bowman Alesha Hart Garfield Hernandez Terry Mckeown Faris Summers Mariah Hilton Yvonne Roche Kian Timms Cherie Armitage Sam Duncan Alexandros Craig Keavy Hook Brogan Gilliam Jodie Parkinson Kianna Kumar Maxwell Rogers Kya Chase Amarah Deleon Danny Martin Syeda Carney Margaret Drummond Luther May Haleema Orr Nelly Sanchez Clarence Cullen Manal Blaese Avaya Roth Krystian Bray Sadiyah Chung Persephone Case Sabina Best Tiffany Wolfe Ayse Alcock Sherri Browning Roy Davey Martyn Rodgers Ryan Diaz Rhyley Wong Flora Hickman Annabel Hutchings Montgomery Chandler Ida Parra Sanah Sinclair Annika Vickers Evan Webb Haider Harrison Kristi Bennett Derren Kirkland Luke Mckay Keziah Daniel Norman Butt Madeleine Walter Jax Clarke Alayah Wilkerson Lauryn Arroyo Darrel Love Eva-Rose Dupont Anastazja Field Leen Plant Melina Berg Austen Rosa Elina Barlow Neelam Goodman Valentino Larsen Chantal Beard Alton Flores Sumayya Wells Emily-Rose Dorsey Muskaan Ball Ruth Shepard Valentina Lawson Robbie Rojas Danyal Sargent Heidi Davis Sukhmani Roberson Rudra Walker Isis Molloy Kaci Cottrell Cherise Naylor Maha Hirst Sidney Sutton Courtney Moore Sara Cook Brennan Barclay Kylo Weir Zahraa Battle Chelsie Wiley Tabitha Liu Katerina Cole Eduard Morgan Bryce Halliday"
RANDOM_WORDS_NAME_POOL = list(set([s for s in [trim(s) for s in _RANDOM_WORDS_NAME_POOL.split(" ")] if s is not None]))
def random_name() -> str: return random_pick(RANDOM_WORDS_NAME_POOL)
def random_names(count: int) -> Set[str]: return random_picks_unique(RANDOM_WORDS_NAME_POOL, count)

_RANDOM_WORDS_NOUN_POOL: str = "instance difference classroom uncle surgery statement beer pie consequence map girlfriend tea interaction preparation maintenance engineering meaning internet office month psychology childhood hearing studio percentage science difficulty woman decision two speaker night elevator shirt instruction tale scene university addition technology week warning income preference leader tradition population courage quality sir combination boyfriend uncle language responsibility clothes blood decision woman girlfriend finding idea debt airport football chocolate solution profession storage cheek night artisan thought child control accident diamond difference supermarket appearance garbage news historian database breath product revenue technology policy honey tennis introduction indication hall importance version topic event reputation employment"
RANDOM_WORDS_NOUN_POOL = list(set([s for s in [trim(s) for s in _RANDOM_WORDS_NOUN_POOL.split(" ")] if s is not None]))
def random_noun() -> str: return random_pick(RANDOM_WORDS_NOUN_POOL)
def random_nouns(count: int) -> Set[str]: return random_picks_unique(RANDOM_WORDS_NOUN_POOL, count)

_RANDOM_WORDS_VERB_POOL: str = "invest wrap announce seal display question love embody feature defend illustrate call admit prepare strain fit fulfil complete cease result remember desire float consult restore care induce pursue sink enforce participate ask plead matter doubt peer object pronounce line couple rub gain exceed research motivate look initiate become originate witness lift supervise test top reveal gain reverse sort purchase administer tax object speed shiver perform spin expect correct arrange hear educate wipe learn expand consist intervene forget leave call remind spill separate cheer complete compare commit train demand need fire explain enforce prove communicate bum disclose trace lower announce flee"
RANDOM_WORDS_VERB_POOL = list(set([s for s in [trim(s) for s in _RANDOM_WORDS_VERB_POOL.split(" ")] if s is not None]))
def random_verb() -> str: return random_pick(RANDOM_WORDS_VERB_POOL)
def random_verbs(count: int) -> Set[str]: return random_picks_unique(RANDOM_WORDS_VERB_POOL, count)

_RANDOM_WORDS_ADJECTIVE_POOL: str = "homeless obeisant scattered coordinated unlikely dirty agreeable futuristic thankful clean abundant somber common medical unwieldy blushing cute dreary nine plain additional accurate pregnant bad wiggly delirious fast colorful hungry noxious zealous probable steady nappy romantic coherent illustrious hissing entertaining psychedelic suitable dysfunctional lewd frail gigantic hard awesome logical acceptable sorry brave apathetic psychotic changeable erratic zany phobic descriptive guiltless sufficient boorish high grandiose rude comfortable ajar ten plant vagabond piquant meaty economic unusual numerous cooing nine auspicious finicky cooperative clumsy maniacal gruesome great melodic magnificent sweltering testy nutty hissing snobbish common youthful conscious sexual kindhearted nappy aloof telling industrious mean"
RANDOM_WORDS_ADJECTIVE_POOL = list(set([s for s in [trim(s) for s in _RANDOM_WORDS_ADJECTIVE_POOL.split(" ")] if s is not None]))
def random_adjective() -> str: return random_pick(RANDOM_WORDS_ADJECTIVE_POOL)
def random_adjectives(count: int) -> Set[str]: return random_picks_unique(RANDOM_WORDS_ADJECTIVE_POOL, count)
# @formatter:on

# endregion Random


# region dataclass


def dataclass_set(obj, **kwargs):
    # https://stackoverflow.com/a/54119384
    for k, v in kwargs.items():
        if not hasattr(obj, k): raise ValueError(f"{obj.__class__.__name__}.{k} does not exist")
        # https://stackoverflow.com/a/54119384
        # setattr(o, k, v)
        object.__setattr__(obj, k, v)
    return obj


# endregion dataclass


# region mixin

def mixin_attrs_dict(self, cls: type) -> Dict[str, Any]:
    name = "_" + cls.__name__ + "__mixin_attrs"
    attrs = getattr(self, name, None)
    if attrs is not None: return attrs
    attrs = dict()
    setattr(self, name, attrs)
    return attrs


def mixin_attrget(self, cls: type, key: str) -> Any | None:
    return mixin_attrs_dict(self, cls).get(key)


def mixin_attrset(self, cls: type, key: str, value) -> Dict[str, Any]:
    d = mixin_attrs_dict(self, cls)
    d[key] = value
    return d


# endregion mixin


# region hash

def _hash(hasher: Callable, value: str | bytes, binary=False):
    if isinstance(value, str):
        value = value.encode()
    h = hasher()
    h.update(value)
    if binary:
        return h.digest()
    else:
        return h.hexdigest()


def hash_md5(value: str | bytes, binary=False) -> str | bytes: return _hash(hasher=hashlib.md5, value=value, binary=binary)


def hash_1(value: str | bytes, binary=False) -> str | bytes: return _hash(hasher=hashlib.sha1, value=value, binary=binary)


def hash_224(value: str | bytes, binary=False) -> str | bytes: return _hash(hasher=hashlib.sha224, value=value, binary=binary)


def hash_256(value: str | bytes, binary=False) -> str | bytes: return _hash(hasher=hashlib.sha256, value=value, binary=binary)


def hash_384(value: str | bytes, binary=False) -> str | bytes: return _hash(hasher=hashlib.sha384, value=value, binary=binary)


def hash_512(value: str | bytes, binary=False) -> str | bytes: return _hash(hasher=hashlib.sha512, value=value, binary=binary)


# endregion hash


# region serialization


def str2base64(s: str | None, encoding: str = "utf-8"):
    if s is None: return None
    return base64.b64encode(s.encode(encoding)).decode(encoding)


def json2base64(json: str | Any | None, indent: int | None = None, encoding: str = "utf-8") -> str | None:
    return str2base64(json2str(json, indent=indent), encoding=encoding)


def json2str(json: str | Any | None, indent: int | None = None) -> str | None:
    if json is None: return None
    if isinstance(json, str):  # already json
        json = json2obj(json)
    return jsn.dumps(json, indent=indent, default=str)


def json2obj(json: str | None) -> Any | None:
    if json is None: return None
    assert isinstance(json, str)
    return jsn.loads(json)


def yaml2base64(yaml: str | Any | None, indent: int | None = None, encoding: str = "utf-8") -> str | None:
    return str2base64(yaml2str(yaml, indent=indent), encoding=encoding)


def yaml2str(yaml: str | Any | None, indent: int | None = None) -> str | None:
    if yaml is None: return None
    if isinstance(yaml, str):  # already yaml
        yaml = yaml2obj(yaml)
    y = YAML(typ='safe')
    return y.dump(yaml)


def yaml2obj(yaml: str | None) -> Any | None:
    if yaml is None: return None
    assert isinstance(yaml, str)
    yaml = YAML(typ='safe')  # default, if not specfied, is 'rt' (round-trip)
    return yaml.load(yaml)


JSON_ROUNDING_DECIMAL_PLACES = 3


class JsonObjectType(Enum):
    STRING = 1
    INT = 2
    FLOAT = 3
    BOOLEAN = 4
    NONE = 5
    DICT = 6
    LIST = 7


class JsonObject:
    def __init__(self, value: Any | None):
        self._json_object_type: JsonObjectType
        self._value = value

        if value is None:
            self._json_object_type = JsonObjectType.NONE

    @property
    def value(self): return self._value

    @property
    def json_object_type(self) -> JsonObjectType: return self.json_object_type


class JsonDict:
    @staticmethod
    def _format_key(key: str | None):
        if key is None: return None
        chars = "\\`*_{}/[]()>< #+-.!$"
        for c in chars:
            if c in key:
                key = key.replace(c, "")
        return trim_casefold(key)

    FORMAT_KEY_FUNC = _format_key

    def __init__(self, json: Mapping[str, Any | None] | str | None):
        self._log = logging.getLogger(__name__)
        self._json_original: Mapping[str, Any | None]
        self._json_original_str: str | None
        self._json_key_formatted: Mapping[str, Any | None]
        self._json_key_formatted_str: str | None

        if json is None:
            self._json_original = {}
            self._json_original_str = None
        elif isinstance(json, Mapping):
            self._json_original = json
            self._json_original_str = json2str(json)
        elif isinstance(json, str):
            self._json_original = json2obj(json)
            self._json_original_str = json
        else:
            msg = f"JSON Type {type(json).__name__} is not supported  {json}"
            self._log.error(f"{self.__class__.__name__}.__init__(json={json}) -> {msg}")
            raise NotImplementedError(msg)
        d = {}
        for k, v in self._json_original.items():
            k = self.__class__.FORMAT_KEY_FUNC(k)
            if k is not None:
                d[k] = v
        self._json_key_formatted = d
        self._json_key_formatted_str = None if self._json_original_str is None else json2str(d)

        self.json_str = self._json_original_str

    def __str__(self):
        return self.json_str

    def __repr__(self):
        return self.__class__.__module__ + "." + self.__class__.__name__ + f"({self._json_original_str})"

    def get_value(self, key: str, value_type: type = None) -> Any | None:
        if key is None: return None
        if len(self._json_original) == 0: return None

        v = self._json_original.get(key)
        if v is None:
            key_formatted = self.__class__.FORMAT_KEY_FUNC(key)
            if key_formatted is not None:
                v = self._json_key_formatted.get(key)
        if v is None: return None
        if value_type is None: return v
        if value_type == str:
            return trim(xstr(v))
        elif value_type == bool:
            v = trim(xstr(v))
            return None if v is None else bool_parse(v)
        elif value_type == Decimal:
            v = trim(xstr(v))
            return None if v is None else decimal_round(Decimal(v), 3)
        elif value_type == int:
            v = trim(xstr(v))
            if v is None: return None
            if "." in v:
                raise ValueError(f"JSON value {key}={v} is a float not an int")
            return int(v)
        elif value_type == float:
            v = trim(xstr(v))
            return None if v is None else float(v)
        elif value_type == UUID:
            v = trim(xstr(v))
            return None if v is None else UUID(v)
        else:
            raise NotImplementedError(f"Parsing to type {value_type.__name__} is not implemented")

    def get_str(self, key: str) -> str | None:
        return self.get_value(key, str)

    def get_int(self, key: str) -> int | None:
        return self.get_value(key, int)

    def get_float(self, key: str) -> float | None:
        return self.get_value(key, float)

    def get_decimal(self, key: str) -> Decimal | None:
        return self.get_value(key, Decimal)

    def get_bool(self, key: str) -> bool | None:
        return self.get_value(key, bool)

    def get_uuid(self, key: str) -> UUID | None:
        return self.get_value(key, UUID)

    def get_list(self, key: str) -> List[Any]:
        lis = self.get_value(key)
        if lis is None: return []
        if not isinstance(lis, List):
            raise TypeError(f"JSON item for key '{key}' is type '{type(lis).__name__}' but not a List: {lis}")
        return lis

    def get_dict(self, key: str) -> JsonDict:
        d = self.get_value(key)
        if d is None: return JsonDict({})
        if not isinstance(d, Mapping):
            raise TypeError(f"JSON item for key '{key}' is type '{type(d).__name__}' but not a dict: {d}")
        return JsonDict(d)

    def items(self):
        return self._json_original.items()


# endregion serialization


# region datetime

def datetime_now_local():
    return datetime.now(timezone.utc).astimezone()


def datetime_now_utc():
    return datetime.utcnow()


def datetime_to_utc(value: datetime | None):
    return None if value is None else value.replace(tzinfo=timezone.utc)


def datetime_to_local(value: datetime | None):
    return None if value is None else datetime_to_utc(value).astimezone()


# endregion datetime


def trim_dict(items: Mapping[str, str | None] | [Tuple[str | None, str | None]] | [[str | None]], exclude_none_values=False) -> Dict[str, str | None]:
    if isinstance(items, Mapping):
        items = [(k, v) for k, v in items.items()]

    result: Dict[str, str | None] = {}
    for item in items:
        k = trim(item[0])
        if k is None: continue
        v = trim(item[0])
        if v is None and exclude_none_values: continue
        result[k] = v

    return result


def print_error(*args, **kwargs):
    """
    https://stackoverflow.com/a/14981125
    """
    print(*args, file=sys.stderr, **kwargs)


def tostring_attributes(obj, included=None, excluded=None, use_repr=False) -> str:
    result = ""
    for name in sorted(obj.__dir__(), key=lambda x: x.casefold()):
        if included is not None and name not in included: continue
        if excluded is not None and name in excluded: continue
        if name.startswith("_"): continue
        if name.startswith("__"): continue
        if not hasattr(obj, name): continue
        v = getattr(obj, name)
        vt = type(v)
        # print(f"Found attribute '{name}'={v}  ({vt.__name__})")
        if vt.__name__ == 'method': continue

        if v is None:
            v = "None"
        elif isinstance(v, str):
            v = "'" + v + "'"
        elif use_repr:
            v = repr(v)
        elif not use_repr:
            v = str(v)

        if len(result) > 0: result += ", "
        result += f"{name}={v}"
    return result


def decimal_round(d: Decimal | float | str, places: int) -> Decimal:
    if not isinstance(d, Decimal):
        d = Decimal(d)
    places *= -1
    # https://stackoverflow.com/a/8869027
    return d.quantize(Decimal(10) ** places).normalize()


def str_convert_to_type(value: Any | None, type_to_convert_to: type, trim_if_string_result: bool = False) -> Any | None:
    if value is None: return None

    if type_to_convert_to == str:
        value_str = value if isinstance(value, str) else str(value)
        if trim_if_string_result:
            value_str = trim(value_str)
        return value_str

    try:
        value_str = trim(str(value))
        if value_str is None:
            return None
        elif type_to_convert_to == bool:
            return bool_parse(value_str)
        elif type_to_convert_to == int:
            return int(value_str)
        elif type_to_convert_to == float:
            return float(value_str)
        else:
            raise NotImplementedError(f"No handler available to convert '{value}' to type {type_to_convert_to.__name__}")
    except ValueError as ve:
        raise ValueError(f"Could not convert '{value}' to type {type_to_convert_to.__name__}") from ve


class DictStrBase(MutableMapping, metaclass=ABCMeta):
    def __init__(self, data=None, **kwargs):
        self._data = dict()
        if data is None: data = {}
        self.update(data, **kwargs)

    @staticmethod
    @abstractmethod
    def _convert_key(s: str) -> str:
        raise NotImplementedError("DictStr._convert_key not implemented")

    def __setitem__(self, key, value):
        self._data[self._convert_key(key)] = (key, value)

    def __getitem__(self, key):
        return self._data[self._convert_key(key)][1]

    def __delitem__(self, key):
        del self._data[self._convert_key(key)]

    def __iter__(self):
        return (casedkey for casedkey, mappedvalue in self._data.values())

    def __len__(self):
        return len(self._data)

    def items_strfunc(self):
        return ((casedkey, keyval[1]) for (casedkey, keyval) in self._data.items())

    def __eq__(self, other):
        if isinstance(other, Mapping):
            other = self.__class__(other)
        else:
            return NotImplemented

        # Compare insensitively
        return dict(self.items_strfunc()) == dict(other.items_strfunc())

    def copy(self):
        return self.__class__(self._data.values())

    def __repr__(self):
        return f"{dict(self.items())}"


class DictStrCase(DictStrBase):
    def __init__(self, data=None, **kwargs):
        super().__init__(data, **kwargs)

    @staticmethod
    def _convert_key(s: str) -> str: return s

    def copy(self) -> DictStrCase:  # Because Self type isn't available yet
        # noinspection PyTypeChecker
        return super().copy()


class DictStrCasefold(DictStrBase):
    def __init__(self, data=None, **kwargs):
        super().__init__(data, **kwargs)

    @staticmethod
    def _convert_key(s: str) -> str: return s.casefold()

    def copy(self) -> DictStrCasefold:  # Because Self type isn't available yet
        # noinspection PyTypeChecker
        return super().copy()


class DictDot(dict):
    """
    https://stackoverflow.com/a/23689767
    dot.notation access to dictionary attributes
    """
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def dictdot(d): return DictDot(d)


dotdict = dictdot


# region atomic


class AtomicIncrement(ABC, Generic[T]):
    @abstractmethod
    def next(self) -> T: raise NotImplemented


class AtomicInt(AtomicIncrement[int]):
    def __init__(self, starting_value=0):
        super(AtomicInt, self).__init__()
        self._value = int(starting_value)
        self._lock = threading.Lock()

    def next(self) -> int:
        with self._lock:
            self._value += int(1)
            return self._value


class AtomicIntNamed:
    def __init__(self, starting_value=0):
        super(AtomicIntNamed, self).__init__()
        self._starting_value = int(starting_value)
        self._names: dict[str, AtomicInt] = {}
        self._lock = threading.Lock()

    def next(self, name: str) -> int:
        with self._lock:
            d = self._names
            if name in d: return d[name].next()
            d[name] = a = AtomicInt(starting_value=self._starting_value)
            return a.next()


_ATOMIC_INT = AtomicInt()
_ATOMIC_INT_NAMED = AtomicIntNamed()


def next_int(name: str = None) -> int: return _ATOMIC_INT.next() if name is None else _ATOMIC_INT_NAMED.next(name)


# endregion atomic

def powerset(iterable):
    """
    powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)
    https://docs.python.org/3/library/itertools.html
    """
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in range(len(s) + 1))


# region profiler

class Profiler:

    def __init__(
            self,
            output: Callable[[str], Any | None] = print,
            name: str = None,
            start: bool = False,
            datetime_format: str | None = "%H:%M:%S.%f"
    ):
        self.output = output
        self.name: str = name if name is not None else self.__class__.__name__ + "[" + str(next_int(self.__class__.__name__)) + "]"
        self.datetime_format = datetime_format
        self.start_perf: float | None = None
        self.start_utc: datetime | None = None
        self.end_perf: float | None = None
        self.end_utc: datetime | None = None
        self.is_started = False
        if start:
            self.start()

    def output_msg(self, msg: str):
        o = self.output
        if o is None: return
        s = ""
        s += self.name if self.name is not None else self.__class__.__name__
        s += " -> "
        s += msg
        o(s)

    def start(self):
        if self.is_started: return
        self.is_started = True
        self.start_utc = datetime_now_utc()
        self.start_perf = time.perf_counter()
        s = "[start]  "
        if self.datetime_format is not None:
            s += " " + self.start_utc.strftime(self.datetime_format)[:-3]
        self.output_msg(s)

    def end(self):
        if not self.is_started: raise ValueError("start() not called yet")
        self.end_perf = time.perf_counter()
        self.end_utc = datetime_now_utc()
        s = "[ end ]  "
        if self.datetime_format is not None:
            s += " " + self.end_utc.strftime(self.datetime_format)[:-3]
        ts = decimal_round(self.end_perf - self.start_perf, 3)
        s += "   " + str(ts) + "s"
        self.output_msg(s)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.end()


def profile(output: Callable[[str], Any | None] = print, name: str = None, show_args: bool = False, datetime_format: str | None = "%H:%M:%S.%f"):
    def decorator(function):
        def wrapper(*args, **kwargs):
            s_parts = ""
            if show_args:
                if args is not None:
                    s_args = trim(f"{args}")
                    if s_args is not None: s_parts += s_args
                if kwargs is not None:
                    s_kwargs = trim(f"{kwargs}")
                    if s_kwargs is not None:
                        if len(s_parts) > 0: s_parts += ", "
                        s_parts += s_kwargs

            n = name
            if name is None:
                n = getattr(function, "__name__", "") + "(" + s_parts + ")"
            else:
                if show_args:
                    n += "(" + s_parts + ")"

            p = Profiler(output=output, datetime_format=datetime_format, name=n)
            p.start()
            result = function(*args, **kwargs)
            p.end()
            return result

        return wrapper

    return decorator


# endregion profiler


# region dataclasses

class DataClassInfoFieldType(NamedTuple):
    type: type
    type_original: type
    name: str
    is_optional: bool
    subtypes: Tuple[DataClassInfoFieldType, ...]

    @property
    def is_union(self):
        return self._is_type(Union, UnionType)

    @property
    def is_list(self):
        return self._is_type(list, List)

    @property
    def is_tuple(self):
        return self._is_type(tuple, Tuple)

    @property
    def is_dict(self):
        return self._is_type(dict, Dict)

    @property
    def is_set(self):
        return self._is_type(set, Set)

    @property
    def is_any(self):
        if self._is_type(Any): return True
        if self.is_union:
            for st in self.subtypes:
                if st.is_any: return True
        return False

    def _is_type(self, *args):
        for t in args:
            if self.type == t: return True
            try:
                if issubclass(self.type, t): return True
            except TypeError:  # typing.Union cannot be used with issubclass()
                pass
        return False

    @classmethod
    def create(cls, type: type | str):
        if isinstance(type, str): type = eval(type)
        type_original = type
        type_origin = get_origin(type)
        is_optional = False
        subtypes = list()
        if type_origin is None:
            mytype = type
        else:
            if isinstance(type_origin, str): type_origin = eval(type)
            mytype = type_origin
            for subtype in get_args(type):
                if subtype == NoneType:
                    is_optional = True
                else:
                    subtypes.append(subtype)

        subtypes_list = [cls.create(t) for t in subtypes]
        if (mytype == Union or mytype == UnionType) and len(subtypes_list) == 1:
            subtype = first(subtypes_list)
            mytype = subtype.type
            name = subtype.name
            subtypes_tuple = subtype.subtypes
        else:
            name = mytype.__name__ if hasattr(mytype, "__name__") else mytype if isinstance(mytype, str) else str(mytype)
            subtypes_tuple = tuple(subtypes_list)

        if mytype == UnionType and name == "UnionType": name = "Union"  # No idea why some are Union and others are UnionType, probably eval

        return cls(
            type=mytype,
            type_original=type_original,
            name=name,
            is_optional=is_optional,
            subtypes=subtypes_tuple,
        )

    def __str__(self):
        s = ""
        if not self.is_union: s += self.name

        if len(self.subtypes) > 0:
            if self.is_union:
                s += " | ".join([str(x) for x in self.subtypes])
            else:
                s += "["
                s += ", ".join([str(x) for x in self.subtypes])
                if s.endswith("]") or s.endswith("]?"): s += " "
                s += "]"
        if self.is_optional: s += "?"
        return s


class DataClassInfoField(NamedTuple):
    field: dataclasses.Field
    name: str
    type: DataClassInfoFieldType

    @classmethod
    def create(cls, field: dataclasses.Field):
        return cls(
            field=field,
            name=field.name,
            type=DataClassInfoFieldType.create(field.type),
        )

    def __str__(self):
        return f"{self.name}: {self.type}"


class DataClassInfo:
    def __init__(self, cls: Any):
        if not inspect.isclass(cls): raise ValueError(f"{cls} is not a class")
        if not self.is_dataclass(cls): raise ValueError(f"Class '{cls.__name__}' is not a dataclass")
        self._type = cls
        self._name = trim(cls.__name__)
        self._module = trim(cls.__module__)
        self._fields: Dict[str, DataClassInfoField] = dict()
        for f in dataclasses.fields(cls):
            self._fields[f.name] = DataClassInfoField.create(f)

    @property
    def type(self):
        return self._type

    @property
    def name(self):
        return self._name

    @property
    def module(self):
        return self._module

    @property
    def fields(self) -> Mapping[str, DataClassInfoField]:
        return self._fields

    @staticmethod
    def is_dataclass(cls: type):
        if type is None: return False
        if not inspect.isclass(cls): return False
        for member_name in dir(cls):
            if "_dataclass_".casefold() in member_name.casefold(): return True
        return False

    def __str__(self):
        prefix = ".".join(s for s in [self.module, self.name] if s is not None)
        suffix = ", ".join(str(f) for f in self.fields.values())
        suffix = coalesce(trim(suffix), "")
        return prefix + "[" + suffix + "]"

    @property
    def str_debug(self):
        s = ".".join(s for s in [self.module, self.name] if s is not None)
        if len(self.fields) == 0: return s
        for f in self.fields.values():
            s += f"\n  {f}"
        return s


def dataclass_info(cls) -> DataClassInfo:
    return DataClassInfo(cls)

def dataclass_infos_in_module(module_name: str) -> [DataClassInfo]:
    result = []
    tpls = inspect.getmembers(sys.modules[module_name])
    for tpl in tpls:
        if not DataClassInfo.is_dataclass(tpl[1]): continue
        dci = DataClassInfo(tpl[1])
        result.append(dci)
    return result


# endregion dataclasses

class Color:
    def __init__(self):
        self._r = 0
        self._g = 0
        self._b = 0
        self._h = 0.0
        self._s = 0.0
        self._v = 0.0

    @property
    def r(self):
        return self._r

    @r.setter
    def r(self, value: int):
        self._r = self._min_max(value, 0, 255)
        self._calc_hsv()

    @property
    def g(self):
        return self._g

    @g.setter
    def g(self, value: int):
        self._g = self._min_max(value, 0, 255)
        self._calc_hsv()

    @property
    def b(self):
        return self._b

    @b.setter
    def b(self, value: int):
        self._b = self._min_max(value, 0, 255)
        self._calc_hsv()

    @property
    def h(self):
        return self._h

    @h.setter
    def h(self, value: float):
        self._h = self._min_max(value, 0.0, 360.0)
        self._calc_rgb()

    @property
    def s(self):
        return self._s

    @s.setter
    def s(self, value: float):
        self._s = self._min_max(value, 0.0, 100.0)
        self._calc_rgb()

    @property
    def v(self):
        return self._v

    @v.setter
    def v(self, value: float):
        self._v = self._min_max(value, 0.0, 100.0)
        self._calc_rgb()

    @staticmethod
    def _min_max(value, min, max):
        if value < min: value = min
        if value > max: value = max
        return value

    def _set_rgb(self, rgb: Tuple):
        self._r = round(self._min_max(rgb[0] * 255.0, 0.0, 255.0))
        self._g = round(self._min_max(rgb[1] * 255.0, 0.0, 255.0))
        self._b = round(self._min_max(rgb[2] * 255.0, 0.0, 255.0))

    def _set_hsv(self, hsv: Tuple):
        self._h = self._min_max(hsv[0] * 360.0, 0.0, 360.0)
        self._s = self._min_max(hsv[1] * 100.0, 0.0, 100.0)
        self._v = self._min_max(hsv[2] * 100.0, 0.0, 100.0)

    def _calc_hsv(self):
        self._set_hsv(colorsys.rgb_to_hsv(self.r / 255.0, self.g / 255.0, self.b / 255.0))

    def _calc_rgb(self):
        self._set_rgb(colorsys.hsv_to_rgb(self.h / 360.0, self.s / 100.0, self.v / 100.0))

    @property
    def hex(self):
        return '#%02x%02x%02x' % (self.r, self.g, self.b)

    @hex.setter
    def hex(self, value):
        h = value.lstrip('#')
        self._set_rgb(tuple(int(h[i:i + 2], 16) for i in (0, 2, 4)))
        self._calc_hsv()


def main():
    pass


if __name__ == "__main__":
    main()
