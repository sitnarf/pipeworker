from abc import abstractmethod
from copy import deepcopy, copy
from dataclasses import dataclass
from enum import Enum, IntEnum, auto
from functools import reduce
import inspect
from itertools import chain
from typing import Any, TypeVar, List, Optional

from toolz import pipe, valmap
from typing_extensions import Protocol
import toolz.curried as c

from pipeworker.cache_engine import \
    CacheEngine, \
    FileCacheEngine, \
    CachedOutput, \
    CodeStateGenerator, \
    CodeState, \
    CacheMissException
from pipeworker.functional import provide_previous
from pipeworker.immutable import set_immutable, immutable_action
from pipeworker.utils import hash_str, log, copy_first_level


class Pipeline:
    def __init__(self, pipeline):
        self.pipeline = pipeline

    def execute(self, data=None):
        return self.pipeline.invoke(InvocationResult(data)).output


@dataclass
class InvocationResult:
    output: Any
    executed: Optional[bool] = None


class Invokable(Protocol):
    @abstractmethod
    def invoke(self, invocation: InvocationResult = None) -> InvocationResult:
        raise NotImplementedError()


class CacheBehaviour(Enum):
    ALWAYS = auto()
    NEVER = auto()
    AUTO = auto()


class LogLevel(IntEnum):
    DISABLED = 0
    ENABLED = 1
    VERBOSE = 2


WithContextSelf = TypeVar("WithContextSelf", bound="WithContext")
WithContextT = TypeVar("WithContextT", bound="WithContext")


class WithContext(Protocol):
    cache_behaviour: CacheBehaviour = CacheBehaviour.AUTO

    cache_engine: CacheEngine = FileCacheEngine()

    log_level: LogLevel = LogLevel.DISABLED

    def set_log_level(self: WithContextSelf, log_level: LogLevel) -> WithContextSelf:
        return self._set_immutable_and_propagate("log_level", log_level)

    def cache(self: WithContextSelf) -> WithContextSelf:
        return self._set_immutable_and_propagate("cache_behaviour", CacheBehaviour.ALWAYS)

    def not_cache(self: WithContextSelf) -> WithContextSelf:
        return self._set_immutable_and_propagate("cache_behaviour", CacheBehaviour.NEVER)

    def set_cache_engine(self: WithContextSelf, cache_engine: CacheEngine) -> WithContextSelf:
        return self._set_immutable_and_propagate("cache_engine", cache_engine)

    def provide_context(self, with_context: WithContextT) -> WithContextT:
        with_context.cache_behaviour = self.cache_behaviour
        with_context.log_level = self.log_level
        with_context.cache_engine = self.cache_engine
        with_context.propagate_context()
        return with_context

    def _set_immutable_and_propagate(
            self: WithContextSelf,
            attribute: str,
            value: Any
    ) -> WithContextSelf:
        updated = set_immutable(self, attribute, value)
        updated.propagate_context()
        return updated

    def propagate_context(self: WithContextSelf) -> None:
        pass


ComponentSelf = TypeVar("ComponentSelf", bound="Component")


class Component(Invokable, WithContext, Protocol):
    _name: Optional[str] = None

    ancestors: List["Component"] = []

    _identifier: Optional[str] = None

    def set_name(self: ComponentSelf, value: str) -> ComponentSelf:
        return set_immutable(self, "_name", value)

    @property
    def name(self) -> str:
        return self._name if self._name else self.__class__.__name__

    @property
    def full_name(self) -> str:
        return pipe(
            self.ancestors,
            c.map(lambda item: item.log_name),
            c.filter(lambda item: item is not None),
            lambda s: chain(s, [self.name]),
            lambda s: " → ".join(s),
        )

    @property
    def log_name(self) -> Optional[str]:
        return self.name

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

    def set_ancestors(self, ancestors):
        self.ancestors = ancestors

    def _generate_identifier_base(self) -> str:
        file = self.__module__
        name = self.__class__.__name__
        return file + "." + name

    def _invoke_node(
            self,
            component: "Component",
            data: Optional[InvocationResult]
    ) -> InvocationResult:
        self.provide_context(component)
        output = component.invoke(data)
        return output


class Group(Component):
    def __init__(self, initialize=None):
        super().__init__(self)
        self.group = []
        if initialize:
            for child in initialize:
                self.append(child, immutable=False)

    @abstractmethod
    def append(self, node: "Node", immutable: bool = True):
        raise NotImplementedError()

    def propagate_context(self) -> None:
        for child in self.group:
            self.provide_context(child)
            child.propagate_context()


class Sequence(Group):
    def invoke(self, invocation: InvocationResult = None):
        return (
            reduce(
                lambda current_data, nodes: (
                    self._invoke_node(
                        nodes.current,
                        current_data
                    )
                ),
                provide_previous(self.group),
                invocation
            )
        )

    def set_ancestors(self, ancestors):
        ancestors = copy(ancestors)
        super().set_ancestors(ancestors)
        for child in self.group:
            child.set_ancestors(ancestors)
            ancestors = [*ancestors, child]

    def append(self, node: "Node", immutable: bool = True):
        appended_node = deepcopy(node)
        self.provide_context(appended_node)
        appended_node.propagate_context()
        if self.group:
            appended_node.set_ancestors(copy(self.group))
        else:
            appended_node.set_ancestors(self.ancestors)
        change = lambda o: o.group.append(appended_node)
        if immutable:
            return immutable_action(self, change)
        else:
            change(self)
            return self

    @property
    def log_name(self) -> Optional[str]:
        return None

    def __copy__(self):
        return copy_first_level(self)

    def __getitem__(self, key: int) -> Component:
        return self.group[key]

    def __or__(self, next_node):
        return self.append(next_node)

    def __and__(self, next_node):
        return Parallel([self, next_node])

    def __iter__(self):
        return iter(self.group)


class Parallel(Group):
    def invoke(self, invocation: InvocationResult = None):
        result = {
            index: self._invoke_node(node, invocation)
            for index, node in enumerate(self.group)
        }
        some_did_changed = pipe(
            result.values(),
            c.map(lambda r: r.executed),
            any,
        )
        return InvocationResult(
            executed=some_did_changed,
            output=valmap(lambda r: r.output, result)
        )

    def provide_context(self, with_context: Any):
        if self.ancestors:
            return self.ancestors[-1].provide_context(with_context)
        else:
            super().provide_context(with_context)
            return with_context

    def set_ancestors(self, ancestors):
        for child in self.group:
            child.set_ancestors(ancestors)

    def append(self, node: "Node", immutable: bool = True):
        appended_node = deepcopy(node)
        self.provide_context(appended_node)
        appended_node.ancestors = self.ancestors
        appended_node.propagate_context()
        change = lambda o: o.group.append(appended_node)
        if immutable:
            return immutable_action(self, change)
        else:
            change(self)
            return self

    @property
    def log_name(self):
        return "(%s)" % " & ".join(map(lambda item: item.log_name, self.group)).strip()

    def __copy__(self):
        return copy_first_level(self)

    def __getitem__(self, key: int) -> Component:
        return self.group[key]

    def __and__(self, next_node):
        return self.append(next_node)

    def __or__(self, next_node):
        return Sequence([self, next_node])

    def __iter__(self):
        return iter(self.group)


@dataclass
class NodeExecutionResponse:
    should_execute: bool
    reason: Optional[str] = None


class Node(Component):
    INPUT_CACHE_KEY = "data"
    CODE_STATE_CACHE_KEY = "code_state"

    def invoke(self, invocation: InvocationResult = None) -> InvocationResult:
        invocation = invocation or InvocationResult(
            output=None,
            executed=None,
        )
        current_code_state = self._get_code_state()
        cached_invocation = self._load_cached_input()
        execution_policy = self._should_execute(
            invocation.executed,
            current_code_state,
            cached_invocation,
        )

        if invocation.output:
            self._save_cached_input(invocation)

        self._log_execution_policy(execution_policy)
        if execution_policy.should_execute:
            output = self.execute(
                invocation.output or (cached_invocation and cached_invocation.output)
            )
            self._save_cached_code_state(current_code_state)
            return InvocationResult(
                output=output,
                executed=cached_invocation == output
            )
        else:
            return InvocationResult(
                None,
                executed=False
            )

    def execute(self, dataset):
        return dataset

    def _should_execute(
            self,
            executed: Optional[bool],
            current_state: CodeState,
            cached_input: Optional[InvocationResult]
    ) -> NodeExecutionResponse:
        if not cached_input:
            return NodeExecutionResponse(True, "Not cached.")
        elif self.cache_behaviour == CacheBehaviour.ALWAYS:
            return NodeExecutionResponse(False, "CacheEngine behaviour: Always.")
        elif self.cache_behaviour == CacheBehaviour.NEVER:
            return NodeExecutionResponse(True, "CacheEngine behaviour: Never.")
        else:
            should_execute = \
                self._did_code_changed(current_state) or \
                executed is True or \
                executed is None
            return NodeExecutionResponse(should_execute)

    def _get_code_state(self):
        module_name = inspect.getmodule(self).__name__

        return CodeStateGenerator(module_name).get()

    def _did_code_changed(self, current_state: CodeState) -> bool:
        cached_state = self._load_cached_code_state()
        if cached_state:
            diff = {k: current_state[k] for k in set(current_state) - set(cached_state)}
        return cached_state == current_state

    def _load_cached_input(self) -> Optional[CachedOutput]:
        try:
            return self.cache_engine.get((self.identifier, self.INPUT_CACHE_KEY))
        except CacheMissException:
            return None

    def _load_cached_code_state(self) -> Optional[CodeState]:
        try:
            return self.cache_engine.get((self.identifier, self.CODE_STATE_CACHE_KEY))
        except CacheMissException:
            return None

    def _save_cached_input(self, data: Any) -> None:
        self.cache_engine.set((self.identifier, self.INPUT_CACHE_KEY), data)

    def _save_cached_code_state(self, value: CodeState) -> None:
        self.cache_engine.set((self.identifier, self.CODE_STATE_CACHE_KEY), value)

    def _log_execution_policy(self, execution_policy: NodeExecutionResponse) -> None:
        if self.log_level >= LogLevel.ENABLED:
            if execution_policy.should_execute:
                log("%s. %s" % (self.full_name, execution_policy.reason))
            else:
                log("Cached. %s. %s" % (self.full_name, execution_policy.reason or ""))

    def __or__(self, next_node):
        return Sequence([self, next_node])

    def __and__(self, next_node):
        return Parallel([self, next_node])
