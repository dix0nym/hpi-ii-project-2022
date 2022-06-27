import logging

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import StringSerializer

from build.gen.gleif.v1 import gleif_pb2
from build.gen.gleif.v1 import relationship_pb2
from build.gen.gleif.v1.gleif_pb2 import LEIRecord
from build.gen.gleif.v1.relationship_pb2 import RelationshipRecord

BOOTSTRAP_SERVER: str = "localhost:29092"
SCHEMA_REGISTRY_URL: str = "http://localhost:8081"
TOPIC_GLEIF: str = "gf-lei"
TOPIC_RELATIONSHIPS: str = "gf-relationship"

log = logging.getLogger(__name__)

class GfProducer:
    def __init__(self, recordType):
        schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        schema = gleif_pb2.LEIRecord if recordType == 'lei' else relationship_pb2.RelationshipRecord
        protobuf_serializer = ProtobufSerializer(
            schema, schema_registry_client, {"use.deprecated.format": True}
        )

        producer_conf = {
            "bootstrap.servers": BOOTSTRAP_SERVER,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": protobuf_serializer,
        }
        self.producer = SerializingProducer(producer_conf)
        self.buffer = 0


    def produce_to_topic(self, record, key):
        topic = None
        if isinstance(record, RelationshipRecord):
            topic = TOPIC_RELATIONSHIPS
        elif isinstance(record, LEIRecord):
            topic = TOPIC_GLEIF
        else:
            log.warning(f'Couldnt detect topic from message type {type(record)}')
            return
        self.producer.produce(
            topic=topic, partition=-1, key=str(key), value=record, on_delivery=self.delivery_report
        )
        self.buffer += 1
        if self.buffer % 10000 == 0:
            self.poll()
  
    def poll(self):
        self.producer.poll()
        self.producer.flush()
        
    @staticmethod
    def delivery_report(err, msg):
        """
        Reports the failure or success of a message delivery.
        Args:
            err (KafkaError): The error that occurred on None on success.
            msg (Message): The message that was produced or failed.
        Note:
            In the delivery report callback the Message.key() and Message.value()
            will be the binary format as encoded by any configured Serializers and
            not the same object that was passed to produce().
            If you wish to pass the original object(s) for key and value to delivery
            report callback we recommend a bound callback or lambda where you pass
            the objects along.
        """
        if err is not None:
            log.error("Delivery failed for User record {}: {}".format(msg.key(), err))
            return
        log.info(
            "User record {} successfully produced to {} [{}] at offset {}".format(
                msg.key(), msg.topic(), msg.partition(), msg.offset()
            )
        )
