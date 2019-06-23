from copy import copy
from typing import Any, TypeVar, Callable


T = TypeVar("T")


def set_immutable(target: T, key: str, value: Any) -> T:
    cloned = copy(target)
    setattr(cloned, key, value)
    return cloned


def immutable_action(target: T, function: Callable) -> T:
    copied = copy(target)
    function(copied)
    return copied
