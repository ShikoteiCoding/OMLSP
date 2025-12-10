import trio

from dataclasses import dataclass
from typing import Any, Generic, TypeVar


type Address = str
type Message = Any
type Response = Any
type DeliveryOptions = str

T = TypeVar("T")


@dataclass
class _Msg(Generic[T]):
    payload: T
    acks_left: int
    done: trio.Event

    def ack(self):
        """
        Should be used with lock for safety.

        Example:
        async with lock:
            msg.ack()
        """
        self.acks_left -= 1
        if self.acks_left == 0:
            self.done.set()


class ChannelT: ...
