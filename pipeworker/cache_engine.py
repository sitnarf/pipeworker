from abc import abstractmethod
import ast
from dataclasses import dataclass
import os
import pickle
import sys
import tempfile
import time
from typing import Any, Iterable, Union, List, Dict

from typing_extensions import Protocol


@dataclass
class CachedOutput:
    code_hash: str
    output: Any


class CacheEngine(Protocol):
    @abstractmethod
    def set(self, key: Union[str, Iterable[str]], value: Any) -> None:
        raise NotImplementedError()

    def get(self, key: Union[str, Iterable[str]]) -> Any:
        raise NotImplementedError()

    def clear(self) -> None:
        raise NotImplementedError()

    @staticmethod
    def _make_key(k: Union[str, Iterable[str]]):
        return k if isinstance(k, str) else "_".join(k)


class FileCacheEngine(CacheEngine):
    def __init__(self, cache_file=None):
        self.cache_file = cache_file

    def set(self, key, value):
        try:
            cache = self._get_cache()
        except CacheMissException:
            cache = {}
        cache[self._make_key(key)] = value
        self._set_cache(cache)

    def get(self, key):
        cache = self._get_cache()
        try:
            return cache[self._make_key(key)]
        except KeyError:
            raise CacheMissException

    def clear(self):
        try:
            os.remove(self._get_file())
        except FileNotFoundError:
            pass

    def _get_file(self):
        if self.cache_file is None:
            self.cache_file = "%s/cache.pickle" % tempfile.gettempdir()
        return self.cache_file

    def _get_resource(self, mode):
        cache_file = self._get_file()
        try:
            return open(cache_file, mode)
        except FileNotFoundError:
            with open(cache_file, "wb+") as file:
                pickle.dump({}, file)
            return open(cache_file, mode)

    def _get_cache(self):
        try:
            with self._get_resource("rb") as file:
                return pickle.load(file)
        except EOFError:
            raise CacheMissException

    def _set_cache(self, cache):
        with self._get_resource("wb+") as file:
            pickle.dump(cache, file)


@dataclass
class ModuleCodeState:
    last_modified: time.struct_time


CodeState = Dict[str, ModuleCodeState]


class CodeStateGenerator:
    modules: Dict = {}
    initial_module: str

    def __init__(self, module_name: str):
        self.initial_module = module_name

    def get(self) -> CodeState:
        self.modules = {}
        self._process(self.initial_module)
        return self.modules

    def _process(self, module_name: str) -> None:
        if module_name in self.modules or module_name in sys.builtin_module_names:
            return
        try:
            module = sys.modules[module_name]
        except KeyError:
            return

        try:
            path = module.__file__
        except AttributeError:
            return

        last_modified = os.path.getmtime(path)
        imports: List[str] = []
        with open(path) as file:
            try:
                root = ast.parse(file.read(), path)
            except UnicodeDecodeError:
                return
            for node in ast.iter_child_nodes(root):
                if isinstance(node, ast.Import):
                    imports.append(node.names[0].name)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    imports.append(node.module)
                else:
                    continue

        self.modules[module_name] = ModuleCodeState(
            last_modified=time.gmtime(last_modified),
        )

        for another_modules in imports:
            self._process(another_modules)


class CacheMissException(Exception):
    pass
