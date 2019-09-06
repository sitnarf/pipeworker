from pipeworker.base import Node
from pipeworker.types import Dataset


class Scikit(Node):
    def __init__(self, scikit):
        self.scikit = scikit

    def fit(self, dataset: Dataset):
        self.scikit.fit(dataset.train[:-1], dataset.train[-1])
        try:
            train_transformed = self.scikit.transform(dataset.train[:-1])
            return dataset.update(train=train_transformed)
        except AttributeError:
            predicted = self.scikit.predict(dataset.predict)
            return dataset.update(predict=predicted)
