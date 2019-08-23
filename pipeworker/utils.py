from copy import copy
from datetime import datetime
import hashlib
from typing import Any, Callable, TypeVar
from unittest.mock import MagicMock

from pipeworker.config import CONFIG


T = TypeVar("T")


def is_primitive(value: Any) -> bool:
    return isinstance(value, (str, bool, int, float))


def hash_str(target) -> str:
    return hashlib.sha224(target.encode('utf-8')).hexdigest()


def mock_method(function: Callable) -> Callable:
    return MagicMock(side_effect=lambda *args, **kwargs: function(None, *args, **kwargs))


def copy_first_level(what: T) -> T:
    copied = type(what)()
    copied.__dict__.update(what.__dict__)

    for attribute in dir(copied):
        value = getattr(what, attribute)
        if not attribute.startswith('__') and isinstance(value, (list, dict)):
            try:
                setattr(copied, attribute, copy(value))
            except AttributeError:
                pass
    return copied


@CONFIG.inject("verbose_level")
def log_old(verbose_level: int, string: str, level: int):
    if verbose_level >= level:
        print(
            "[%s] %s" %
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), string))


def log(string: str):
    print(string)


RED = '\033[91m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
LIGHT_PURPLE = '\033[94m'
PURPLE = '\033[95m'
GRAY = '\033[97m'
END = '\033[0m'


def gray(s):
    return GRAY + s + END


class NestedDict(dict):
    def __missing__(self, key):
        self[key] = NestedDict()
        return self[key]
