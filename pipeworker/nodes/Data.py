from copy import copy
from pydash import assign
from pipeworker.base import Node


class Data(Node):

    def __init__(self, data):
        self.data = data

    def fit(self, input_data):
        return (
            assign(copy(input_data), self.data) if input_data
            else self.data
        )
