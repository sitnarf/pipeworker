from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from functools import reduce
import inspect
from typing import Any, TypeVar, List, Optional

from toolz import pipe, valmap
from typing_extensions import Protocol
import toolz.curried as c

from pipeworker.cache import \
    Cache, \
    FileCache, \
    CachedOutput, \
    CodeStateGenerator, \
    CodeState, \
    CacheMissException
from pipeworker.functional import provide_previous
from pipeworker.immutable import set_immutable, immutable_action
from pipeworker.utils import hash_str


class Pipeline:
    def __init__(self, pipeline):
        self.pipeline = pipeline

    def execute(self, data=None):
        return self.pipeline.invoke(InvocationData(data)).output


@dataclass
class InvocationData:
    output: Any
    did_change: bool = False


class Invokable(Protocol):
    @abstractmethod
    def invoke(self, data: InvocationData) -> InvocationData:
        raise NotImplementedError()


WithContextSelf = TypeVar("WithContextSelf", bound="WithContext")
WithContextT = TypeVar("WithContextT", bound="WithContext")


class CacheBehaviour(Enum):
    ALWAYS = auto()
    NEVER = auto()
    AUTO = auto()


class WithContext(Protocol):
    cache_behaviour: CacheBehaviour = CacheBehaviour.AUTO

    cache_engine: Cache = FileCache()

    _ancestors: List[str] = []

    def cache(self: WithContextSelf) -> WithContextSelf:
        return set_immutable(self, "cache_behaviour", CacheBehaviour.ALWAYS)

    def not_cache(self: WithContextSelf) -> WithContextSelf:
        return set_immutable(self, "cache_behaviour", CacheBehaviour.NEVER)

    def set_cache_engine(self: WithContextSelf, cache_engine: Cache) -> WithContextSelf:
        self.cache_engine = cache_engine
        return self

    def _provide_context(self, with_context: WithContextT) -> WithContextT:
        with_context.cache_behaviour = self.cache_behaviour
        return with_context


ComponentSelf = TypeVar("ComponentSelf", bound="Component")


class Component(Invokable, WithContext, Protocol):
    name: str

    _identifier: Optional[str] = None

    def set_name(self: ComponentSelf, value: str) -> ComponentSelf:
        return set_immutable(self, "name", value)

    @property
    def identifier(self):
        if not self._identifier:
            return hash_str(self._generate_identifier_base())
        else:
            return self._identifier

    def update_identifier(self: ComponentSelf, context_identifier: Optional[str]) -> ComponentSelf:
        return set_immutable(
            self,
            "_identifier",
            hash_str(
                (context_identifier + "." if context_identifier else "") +
                self._generate_identifier_base()
            )
        )

    def _generate_identifier_base(self) -> str:
        file = self.__module__
        name = self.__class__.__name__
        return file + "." + name


class Group(Component, Protocol):
    def invoke_block(self, component: Component, data: InvocationData) -> InvocationData:
        with_context = self._provide_context(component)
        output = with_context.invoke(data)
        return output


class Sequence(Group):
    def __init__(self, initialize=None):
        self.sequence = initialize if initialize else []

    def invoke(self, data: InvocationData):
        return (
            reduce(
                lambda current_data, blocks: (
                    self.invoke_block(
                        blocks.current,
                        current_data
                    )
                ),
                provide_previous(self.sequence),
                data
            )
        )

    def append(self, block):
        return immutable_action(self, lambda o: o.sequence.append(block))

    def __or__(self, next_block):
        return self.append(next_block)

    def __and__(self, next_block):
        return Parallel([self, next_block])

    def __iter__(self):
        return iter(self.sequence)


class Parallel(Group):
    def __init__(self, initialize=None):
        self.parallel = initialize if initialize else []

    def invoke(self, data: InvocationData):
        result = {
            index: self.invoke_block(block, data)
            for index, block in enumerate(self.parallel)
        }
        some_did_changed = pipe(
            result.values(),
            c.map(lambda r: r.did_change),
            any,
        )
        return InvocationData(
            did_change=some_did_changed,
            output=valmap(lambda r: r.output, result)
        )

    def append(self, block):
        return immutable_action(self, lambda o: o.parallel.append(block))

    def __and__(self, next_block):
        return self.append(next_block)

    def __or__(self, next_block):
        return Sequence([self, next_block])

    def __iter__(self):
        return iter(self.parallel)


class Block(Component):
    DATA_CACHE_KEY = "data"
    CODE_STATE_CACHE_KEY = "code_state"

    def invoke(self, data: InvocationData) -> InvocationData:
        current_code_state = self._get_code_state()
        cached_invocation = self._load_cached_data()
        if self._should_execute(data.did_change, current_code_state, cached_invocation):
            output = self.execute(data.output)
            self._save_cached_data(output)
            self._save_cached_code_state(current_code_state)
            return InvocationData(
                output=output,
                did_change=cached_invocation.output == output if cached_invocation else True
            )
        else:
            return InvocationData(
                cached_invocation.output if cached_invocation else None,
                did_change=True
            )

    def execute(self, dataset):
        return dataset

    def _should_execute(
            self,
            did_input_change: bool,
            current_state: CodeState,
            cached_invocation: Optional[InvocationData]
    ) -> bool:
        if not cached_invocation:
            return True
        elif self.cache_behaviour == CacheBehaviour.ALWAYS:
            return False
        elif self.cache_behaviour == CacheBehaviour.NEVER:
            return True
        else:
            return self._did_code_changed(current_state) or did_input_change

    def _get_code_state(self):
        module_name = inspect.getmodule(self).__name__
        return CodeStateGenerator(module_name).get()

    def _did_code_changed(self, current_state: CodeState) -> bool:
        cached_state = self._load_cached_code_state()
        if cached_state:
            diff = {k: current_state[k] for k in set(current_state) - set(cached_state)}
            print("diff", diff)
        return cached_state == current_state

    def _load_cached_data(self) -> Optional[CachedOutput]:
        try:
            return self.cache_engine.get((self.identifier, self.DATA_CACHE_KEY))
        except CacheMissException:
            return None

    def _load_cached_code_state(self) -> Optional[CodeState]:
        try:
            return self.cache_engine.get((self.identifier, self.CODE_STATE_CACHE_KEY))
        except CacheMissException:
            return None

    def _save_cached_data(self, data: Any) -> None:
        self.cache_engine.set((self.identifier, self.DATA_CACHE_KEY), data)

    def _save_cached_code_state(self, value: CodeState) -> None:
        self.cache_engine.set((self.identifier, self.CODE_STATE_CACHE_KEY), value)

    def __or__(self, next_block):
        return Sequence([self, next_block])

    def __and__(self, next_block):
        return Parallel([self, next_block])
