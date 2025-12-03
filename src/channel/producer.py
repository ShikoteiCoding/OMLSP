from typing import Generic, TypeVar


from channel.channel import Channel
from channel.types import Address

T = TypeVar("T")


class Producer(Generic[T]):
    #: Address for production
    address: Address

    #: Channel
    channel: Channel[T]

    def __init__(self, address: Address, channel: Channel[T]):
        self.address = address
        self.channel = channel

    async def produce(self, data: T):
        await self.channel.send(data)
