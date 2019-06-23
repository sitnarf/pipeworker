from datetime import datetime
import hashlib
from typing import Any, Callable
from unittest.mock import MagicMock

from pipeworker.config import CONFIG


def is_primitive(value: Any) -> bool:
    return isinstance(value, (str, bool, int, float))


def hash_str(target) -> str:
    return hashlib.sha224(target.encode('utf-8')).hexdigest()


def mock_method(function: Callable) -> Callable:
    return MagicMock(side_effect=lambda *args, **kwargs: function(None, *args, **kwargs))


@CONFIG.inject("verbose_level")
def log(verbose_level: int, string: str, level: int):
    if verbose_level >= level:
        print(
            "[%s] %s" %
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), string))
