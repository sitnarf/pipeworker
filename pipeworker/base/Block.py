from pipeworker.base.Parallel import Parallel
from pipeworker.base.Sequence import Sequence


class Block:

    name: str

    def invoke(self, dataset):
        return self.execute(dataset)

    def execute(self, dataset):
        return dataset

    def set_name(self, value: str) -> 'Block':
        self.name = value
        return self

    def __or__(self, next_block):
        return (
            Sequence([self, next_block])
        )

    def __and__(self, next_block):
        return Parallel([self, next_block])
