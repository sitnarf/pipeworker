from copy import copy
from pydash import assign
from pipeworker.base.Block import Block


class Data(Block):

    def __init__(self, data):
        self.data = data

    def execute(self, input_data):
        return (
            assign(copy(input_data), self.data) if input_data
            else self.data
        )
