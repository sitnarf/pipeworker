from abc import ABC, abstractmethod
from typing import Any


class Invokable(ABC):

    @abstractmethod
    def invoke(self, data: Any) -> Any: pass
