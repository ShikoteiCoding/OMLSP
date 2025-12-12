from __future__ import annotations

from collections import defaultdict
from loguru import logger
from typing import TypeVar

from channel.types import Address, Message
from channel.channel import Channel
from channel.consumer import Consumer
from channel.producer import Producer
from channel.promise import Promise

T = TypeVar("T")


class ChannelBroker:
    """
    Global singleton ChannekBroker.

    NOTE: For now everything is unique (consumers, producers & channels)
    This is because trio.membuffer are not broadcast variables anyway.

    """

    #: Flag to detect init
    _instanciated: bool = False

    #: Singleton instance
    _instance: ChannelBroker

    #: Consumers (unique per address)
    _consumers: dict[Address, Consumer] = {}

    #: Producers (unique per address)
    _producers: dict[Address, Producer] = {}

    #: Channels (unique per address)
    _channels: dict[Address, Channel] = {}

    #: Round-robin track
    # NOTE: could be an assignment strategy per consumer
    _round_robin: defaultdict[Address, int] = defaultdict(int)

    def __init__(self):
        raise NotImplementedError("Singleton. Please use get_instance.")

    def init(self):
        pass

    @classmethod
    def get_instance(cls) -> ChannelBroker:
        if cls._instanciated:
            logger.info("Instance already exist.")
            return cls._instance

        cls._instance = cls.__new__(cls)
        cls._instance.init()
        cls._instanciated = True

        return cls._instance

    def consumer(self, address: Address) -> Consumer:
        """
        Creates or get existing consumer for given address
        """
        channel = self._channels.get(address)

        if not channel:
            channel = Channel(100)
            self._channels[address] = channel
            logger.debug("Channel created for address '{}'", address)

        consumer = self._consumers.get(address)

        if not consumer:
            consumer = Consumer(address, channel)
            logger.debug("Consumer created for address '{}'", address)

        self._consumers[address] = consumer

        return consumer

    def producer(self, address: Address) -> Producer:
        """
        Creates or get existing producer for given address.
        """
        channel = self._channels.get(address)

        if not channel:
            channel = Channel(100)
            self._channels[address] = channel
            logger.debug("Channel created for address '{}'", address)

        producer = self._producers.get(address)

        if not producer:
            producer = Producer(address, channel)
            logger.debug("Producer created for address '{}'", address)

        self._producers[address] = producer

        return producer

    async def publish(self, address: Address, message: Message):
        """
        Pub/Sub pattern for unique consumer.

        Allows for anonymous publish without keeping a producer client.

        The consumer isn't expected to reply.
        """
        producer = self.producer(address)
        logger.debug("Message published to '{}': '{}'", address, message)
        await producer.produce(message)

    async def request[T](self, address: Address, message: Message[T]):
        """
        Request/Response pattern. Round robin delivery if multiple consumers.

        A receiver is expected to reply if request is received.
        """
        raise NotImplementedError("Not the most useful for now.")

    async def send[T](self, address: Address, message: Message) -> T:
        """
        Point-to-point pattern.
        Returns the data directly on valid response.
        Raises exception on invalid response.
        """
        promise = Promise[T]()
        await self.publish(address, (message, promise))
        return await promise.await_result()


def _get_channel_broker() -> ChannelBroker:
    return ChannelBroker.get_instance()
