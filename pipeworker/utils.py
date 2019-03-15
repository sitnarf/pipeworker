from datetime import datetime
from typing import Any
from pipeworker.config import config


def is_primitive(value: Any) -> bool:
    return type(value) in (str, bool, int, float)


@config.inject("verbose_level")
def log(verbose_level: int, string: str, level: int):
    if verbose_level >= level:
        print("[%s] %s" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), string))
