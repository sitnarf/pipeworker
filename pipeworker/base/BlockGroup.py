from typing import Any, Callable, Union
from pipeworker.types.Invokable import Invokable


class BlockGroup:

    @staticmethod
    def invoke_block(component: Union[Callable, Invokable], data: Any) -> Any:
        try:
            return component(data)
        except TypeError:
            return component.invoke(data)
