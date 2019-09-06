from pipeworker.base import Node
from pipeworker.types import Dataset
from sklearn.model_selection import train_test_split


class TrainTestSplit(Node):
    def __init__(self, **params):
        self.params = params

    def fit(self, dataset: Dataset):
        (train, predict) = train_test_split(dataset.data, **self.params)
        return dataset.update(
            train=train,
            predict=predict,
        )
