from collections import defaultdict

from pipeworker.functions.utils import dict_deep_merge
from pipeworker.types.Mergable import Mergable
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Union, TypeVar
from pandas import DataFrame


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
