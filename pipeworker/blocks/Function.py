from typing import Callable, Any

from pipeworker.base.Block import Block


class Function(Block):

    def __init__(self, function: Callable[[Any], Any]):
        self.function = function

    def execute(self, data):
        return self.function(data)
