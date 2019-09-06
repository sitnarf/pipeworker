from typing import Callable, Any

from pipeworker.base import Node


class Function(Node):

    def __init__(self, function: Callable[[Any], Any]):
        self.function = function

    def fit(self, data):
        return self.function(data)
