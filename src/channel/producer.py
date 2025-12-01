

class Producer(Generic[T]):
    #: Ref to event bus for callback
    event_bus: ChannelBroker

    #: Address for production
    address: Address

    #: Channel
    channel: Channel[T]

    def __init__(self, event_bus: ChannelBroker, address: Address, channel: Channel[T]):
        self.event_bus = event_bus
        self.address = address
        self.channel = channel

    async def produce(self, data: T):
        await self.channel.send(data)