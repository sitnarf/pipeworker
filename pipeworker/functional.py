from dataclasses import dataclass
from typing import Iterable, TypeVar, Generic, Optional


T = TypeVar('T')


@dataclass
class PreviousCurrent(Generic[T]):
    previous: Optional[T]
    current: T


def provide_previous(iterable: Iterable[T]) -> Iterable[PreviousCurrent[T]]:
    last_item = None
    for item in iterable:
        yield PreviousCurrent(last_item, item)
        last_item = item
