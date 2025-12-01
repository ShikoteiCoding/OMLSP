from typing import Generic, TypeVar

from channel.types import Address
from channel.channel import Channel

T = TypeVar("T")


class Consumer(Generic[T]):
    #: Address for consumption
    address: Address

    #: Channel
    channel: Channel[T]

    def __init__(self, address: Address, channel: Channel[T]):
        self.address = address
        self.channel = channel

    async def consume(self):
        # NOTE: not strictly a consume as it is waiting
        return await self.channel.recv()
