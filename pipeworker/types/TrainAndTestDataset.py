from pipeworker.types.Mergable import Mergable
from dataclasses import dataclass, field
from pandas import DataFrame


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
