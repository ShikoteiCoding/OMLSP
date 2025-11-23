from __future__ import annotations

import polars as pl

from typing import Awaitable, Callable


class LookupCallbackStore:
    #: Flag to detect init
    _instanciated: bool = False

    #: Singleton instance
    _instance: LookupCallbackStore

    #: Internal store of the instance
    _store: dict[str, Callable[[pl.DataFrame], Awaitable[pl.DataFrame]]]

    def __init__(self):
        raise NotImplementedError("Singleton, use `get_instance`")

    def init(self):
        self._store = {}

    @classmethod
    def get_instance(cls) -> LookupCallbackStore:
        if cls._instanciated:
            return cls._instance

        cls._instance = cls.__new__(cls)
        cls._instance.init()
        cls._instanciated = True

        return cls._instance

    def add(
        self, name: str, func: Callable[[pl.DataFrame], Awaitable[pl.DataFrame]]
    ) -> None:
        self._instance._store[name] = func

    def delete(self, name: str):
        del self._instance._store[name]

    def get_by_names(
        self, names: list[str]
    ) -> dict[str, Callable[[pl.DataFrame], Awaitable[pl.DataFrame]]]:
        return {name: func for name, func in self._store.items() if name in names}


callback_store = LookupCallbackStore.get_instance()
