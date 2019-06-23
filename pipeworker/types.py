from abc import ABC, abstractmethod
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Union
from typing import TypeVar

from pandas import DataFrame

from pipeworker.functions.utils import dict_deep_merge


class Mergable(ABC):
    @abstractmethod
    def merge(self, what): pass


class Invokable(ABC):
    @abstractmethod
    def invoke(self, data: Any) -> Any: pass


@dataclass
class TrainAndTestDataset(Mergable):
    train: DataFrame
    test: DataFrame
    label: str = None
    payload: dict = field(default_factory=lambda: {})

    def merge(self, what: 'TrainAndTestDataset') -> 'TrainAndTestDataset':
        return TrainAndTestDataset(
            train=self.train.join(what.train),
            test=self.test.join(what.test),
            label=self.label,
            payload={**self.payload, **what.payload}
        )


T = TypeVar('T')


@dataclass
class Dataset(Mergable):
    data: T
    train: T = None
    predict: T = None
    label: str = None
    payload: dict = field(default_factory=lambda: defaultdict(lambda: defaultdict(int)))

    def merge(self, what: Union['Dataset', DataFrame]) -> 'Dataset':
        frame_to_merge = what.data if isinstance(what, Dataset) else what
        return Dataset(
            data=self.data.join(frame_to_merge),
            label=self.label,
            payload=deepcopy(self.payload)
        )

    def update(self, deep_merge: bool = False, **parameters):
        return Dataset(
            **(
                {
                    **vars(self),
                    **parameters,
                } if not deep_merge else
                dict_deep_merge(
                    vars(self),
                    parameters,
                )
            )
        )

    def update_payload(self, **payload):
        pass
