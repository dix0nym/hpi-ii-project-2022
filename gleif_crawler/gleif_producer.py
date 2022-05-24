import logging

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import StringSerializer

from gleif.v1 import gleif_pb2
from gleif.v1.gleif_pb2 import LEIRecord
from gleif.v1.relationship_pb2 import RelationshipRecord

BOOTSTRAP_SERVER: str = "localhost:29092"
SCHEMA_REGISTRY_URL: str = "http://localhost:8081"
TOPIC_GLEIF: str = "gleif"
TOPIC_RELATIONSHIPS: str = "gleif_relationships"

log = logging.getLogger(__name__)

class GleifProducer:
    def __init__(self):
        print('init GleifPRoducer')
        schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        protobuf_serializer = ProtobufSerializer(
            gleif_pb2.LEIRecord, schema_registry_client, {"use.deprecated.format": True}
        )

        producer_conf = {
            "bootstrap.servers": BOOTSTRAP_SERVER,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": protobuf_serializer,
        }
        self.producer = SerializingProducer(producer_conf)

    def produce_to_topic(self, record):
        topic = None
        key = None
        if isinstance(record, RelationshipRecord):
            topic = TOPIC_RELATIONSHIPS
            key = f"{record.Relationship.StartNode.NodeID}_{record.Relationship.EndNode.NodeID}_\
                    {record.Registration.RegistrationStatus}_{record.Relationship.RelationshipType}_\
                    {record.Registration.InitialRegistrationDate.seconds}"
        elif isinstance(record, LEIRecord):
            key = record.LEI
            topic = TOPIC_GLEIF
        if not topic:
            log.warning(f'Couldnt detect topic from message type {type(record)}')
        self.producer.produce(
            topic=topic, partition=-1, key=str(key), value=record, on_delivery=self.delivery_report
        )
        self.producer.poll()
  
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