"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""

        bootstrap_servers = 'PLAINTEXT://localhost:9092'
        schema_registry = 'http://localhost:8081'

        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas


        self.admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
        self.broker_properties = {
            "bootstrap.servers" : bootstrap_servers,
            'schema.registry.url': schema_registry
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
             self.broker_properties,
             default_key_schema = self.key_schema,
             default_value_schema = self.value_schema
         )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""


        new_topics = [NewTopic(self.topic_name, num_partitions = self.num_partitions, replication_factor = self.num_replicas)]

        if self.check_topic_exists(topic_name = self.topic_name):
            logger.info("topic {} already exists".format(self.topic_name))
            return

        fs = self.admin_client.create_topics(new_topics)

        for topic, f in fs.items():
            try:
                f.result()
                logger.info("Topic {} created".format(topic))
            except Exception as e:
                logger.info("Failed to create topic {}: {}".format(topic, e))


    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        if self.producer is None:
            return
        self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

    def check_topic_exists(self,topic_name):
        cluster_metadata = self.admin_client.list_topics(topic = topic_name)
        return len(cluster_metadata.topics) !=0