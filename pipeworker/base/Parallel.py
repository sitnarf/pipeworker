from pipeworker.base.BlockGroup import BlockGroup


class Parallel(BlockGroup):

    def __init__(self, initialize=None):
        self.parallel = initialize if initialize else []

    def invoke(self, data=None):
        return {
            index: self.invoke_block(block, data)
            for index, block in enumerate(self.parallel)
        }

    def append(self, block):
        self.parallel.append(block)
        return self

    def get_parallel(self):
        return self.parallel

    def __and__(self, next_block):
        self.append(next_block)
        return self

    def __or__(self, next_block):
        from pipeworker.base.Sequence import Sequence
        return Sequence([self, next_block])

    def __iter__(self):
        return iter(self.parallel)
