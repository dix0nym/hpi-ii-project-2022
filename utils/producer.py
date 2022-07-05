import logging

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import StringSerializer

log = logging.getLogger(__name__)

BOOTSTRAP_SERVER: str = "localhost:29092"
SCHEMA_REGISTRY_URL: str = "http://localhost:8081"

class Producer:
    def __init__(self, topic, schema):
        self.schema = schema

        schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        protobuf_serializer = ProtobufSerializer(
            self.schema, schema_registry_client, {"use.deprecated.format": True}
        )

        producer_conf = {
            "bootstrap.servers": BOOTSTRAP_SERVER,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": protobuf_serializer,
        }
        self.topic = topic
        self.producer = SerializingProducer(producer_conf)
        self.buffer = 0
    
    def produce(self, obj, key):
        self.producer.produce(
            topic=self.topic, partition=-1, key=str(key), value=obj, on_delivery=self.delivery_report
        )
        self.buffer += 1
        if self.buffer % 10000 == 0:
            self.poll()

    def poll(self):
        self.producer.poll()
        self.producer.flush()

    @staticmethod
    def delivery_report(err, msg):
        if err is not None:
            log.error("Delivery failed for User record {}: {}".format(msg.key(), err))
            return

        log.info(
            "User record {} successfully produced to {} [{}] at offset {}".format(
                msg.key(), msg.topic(), msg.partition(), msg.offset()
            )
        )