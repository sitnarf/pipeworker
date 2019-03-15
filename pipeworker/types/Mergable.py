from abc import ABC, abstractmethod


class Mergable(ABC):
    @abstractmethod
    def merge(self, what): pass
