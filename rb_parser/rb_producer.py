import logging

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import StringSerializer

from build.gen.parsed_hrb.v1 import company_pb2, ceo_pb2, address_pb2, owning_company_pb2
from build.gen.parsed_hrb.v1.company_pb2 import Company, CompanyStatus
from build.gen.parsed_hrb.v1.ceo_pb2 import CEO
from build.gen.parsed_hrb.v1.address_pb2 import Address
from build.gen.parsed_hrb.v1.owning_company_pb2 import owning_company

log = logging.getLogger(__name__)

BOOTSTRAP_SERVER: str = "localhost:29092"
SCHEMA_REGISTRY_URL: str = "http://localhost:8081"
TOPIC: str = "corporate-events-parsed"

class RbProducer:
    def __init__(self, recordType):
        schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        schema = Company
        if recordType == 'address': 
            schema = Address
        elif recordType == 'owning_company': 
            schema = owning_company
        elif recordType == 'CEO': 
            schema = CEO
        
        self.topic = recordType
        
        protobuf_serializer = ProtobufSerializer(
            schema, schema_registry_client, {"use.deprecated.format": True}
        )

        producer_conf = {
            "bootstrap.servers": BOOTSTRAP_SERVER,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": protobuf_serializer,
        }

        self.producer = SerializingProducer(producer_conf)

    def produce_to_topic(self, record):
        self.producer.produce(
            topic=self.topic, partition=-1, key=str(record.id), value=record, on_delivery=self.delivery_report
        )

        # It is a naive approach to flush after each produce this can be optimised
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
