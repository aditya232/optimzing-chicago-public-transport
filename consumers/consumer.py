"""Defines core consumer functionality"""
import logging

from confluent_kafka import Consumer,OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest
        bootstrap_servers = 'PLAINTEXT://localhost:9092'
        schema_registry = 'http://localhost:8081'

        self.broker_properties = {
            "bootstrap.servers" : bootstrap_servers,
            'schema.registry.url': schema_registry
        }

        # TODO: Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer({
                'bootstrap.servers': bootstrap_servers,
                'group.id': 'org.chicago.cta',
                'schema.registry.url': 'http://127.0.0.1:8081'
                })
        else:
            self.consumer = Consumer({
                'bootstrap.servers': bootstrap_servers,
                'group.id': 'org.chicago.cta',
            })

        self.consumer.subscribe([topic_name_pattern], on_assign = self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        for partition in partitions:
            if self.offset_earliest:
                partition.offset = OFFSET_BEGINNING
        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        #
        #
        # TODO: Poll Kafka for messages. Make sure to handle any errors or exceptions.
        # Additionally, make sure you return 1 when a message is processed, and 0 when no message
        # is retrieved.
        #

        try :
            msg = self.consumer.poll(1.0)
            if msg is None:
                return 0
            if msg.error():
                logger.error("error for message {}: {}".format(msg, msg.error))
                return 0
            self.message_handler(msg)
            return 1
        except SerializerError as e:
            logger.error("Message deserialization failed for {}: {}".format(msg, e))

    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()
