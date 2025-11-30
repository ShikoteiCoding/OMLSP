from __future__ import annotations

from collections import defaultdict
from loguru import logger
from typing import Any, Awaitable, Callable, Generic, TypeVar

from eventbus.types import Address, DeliveryOptions, Message, Response
from channel import Channel


T = TypeVar("T")
type MessageHandler = Callable[[Message], Awaitable[Response] | None]
type ReplyHandler = Any


class Consumer(Generic[T]):
    #: Ref to event bus for callback
    event_bus: EventBus

    #: Address for consumption
    address: Address

    #: Message handler
    # handler: MessageHandler

    #: Channel
    channel: Channel

    def __init__(
        self, event_bus: EventBus, address: Address, channel: Channel
    ):  # , handler: MessageHandler):
        self.event_bus = event_bus
        self.address = address
        # self.handler = handler
        self.channel = channel


class Producer(Generic[T]):
    #: Ref to event bus for callback
    event_bus: EventBus

    #: Address for production
    address: Address

    #: Channel
    channel: Channel

    def __init__(self, event_bus: EventBus, address: Address, channel: Channel):
        self.event_bus = event_bus
        self.address = address
        self.channel = channel

    async def produce(self, data: T):
        await self.channel.send(data)


class EventBus:
    """
    Global singleton EventBus.

    Use to communicate accross
    """

    #: Flag to detect init
    _instanciated: bool = False

    #: Singleton instance
    _instance: EventBus

    #: Consumers
    _consumers: defaultdict[Address, list[Consumer]] = defaultdict(list)

    #: Producer
    _producer: defaultdict[Address, list[Producer]] = defaultdict(list)

    #: Channel store
    _channels: dict[Address, Channel] = {}

    #: Round-robin track
    # NOTE: could be an assignment strategy per consumer
    _round_robin: defaultdict[Address, int] = defaultdict(int)

    def __init__(self):
        raise NotImplementedError("Singleton. Please use get_instance.")

    def init(self):
        pass

    @classmethod
    def get_instance(cls) -> EventBus:
        if cls._instanciated:
            logger.info("Instance already exist.")
            return cls._instance

        cls._instance = cls.__new__(cls)
        cls._instance.init()
        cls._instanciated = True

        return cls._instance

    def consumer(self, address: Address) -> Consumer:
        """
        Creates a consumer for given address
        """
        channel = self._channels.get(address)

        if not channel:
            channel = Channel(100)
            self._channels[address] = channel

        consumer = Consumer(self, address, channel)

        self._consumers[address].append(consumer)

        logger.debug("Consumer created on address '{}'", address)
        return consumer

    def producer(self, address: Address) -> Producer:
        """
        Creates a producer for given address
        """
        channel = self._channels.get(address)

        if not channel:
            channel = Channel(100)
            self._channels[address] = channel

        producer = Producer(self, address, channel)
        self._producer[address].append(producer)

        logger.debug("Producer created on address '{}'", address)
        return producer

    async def publish[T](
        self, address: Address, message: Message, options: DeliveryOptions | None = None
    ):
        """
        Pub/Sub pattern (i.e mutliple consumer is possible).

        A receiver isn't expected to reply.
        """
        consumers = self._consumers[address]

        if not consumers:
            return  # ra

        # publish to all registered handlers
        for consumer in consumers:
            logger.error("event received: {}", message)
            await consumer.channel.send(message)

        return

    async def request[T](
        self, address: Address, message: Message[T], reply_handler: ReplyHandler
    ):
        """
        Request/Response pattern. Round robin delivery if multiple consumers.

        A receiver is expected to reply if request is received.
        """
        consumers = self._consumers[address]

        if not consumers:
            return  # raise error

        # select receiver through round-robin
        consumer = consumers[self._round_robin[address] % len(consumers)]

        await consumer.channel.send(message)

    async def send[T](self, address: Address, message: Message):
        """
        Point-to-point pattern. Round robin delivery if multiple consumers.

        A consumer can .reply() to the message but isn't obligated
        """
        raise NotImplementedError("Not the most useful for now.")


def _get_event_bus() -> EventBus:
    return EventBus.get_instance()
