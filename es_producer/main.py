import os
import sys
import logging 
from es_producer import ESProducer
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient

from build.gen.bakdata.corporate.v1 import corporate_pb2
from build.gen.bakdata.corporate.v1.corporate_pb2 import Corporate

from build.gen.gleif.v1 import gleif_pb2
from build.gen.gleif.v1 import relationship_pb2
from build.gen.gleif.v1.gleif_pb2 import LEIRecord
from build.gen.gleif.v1.relationship_pb2 import RelationshipRecord


logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

def main():
    args = sys.argv[1:]
    if len(args) != 2:
        exit(f"missing args: {sys.argv[0]} [index] [topic]")

    index = args[0]
    topic = args[1]
    schema = None

    if topic == 'gleif-neo4j':
        schema = gleif_pb2.LEIRecord
    elif topic == 'corporate-events-neo4j':
        schema = corporate_pb2.Corporate
    elif topic == 'gleif_relationships-neo4j':
        schema == relationship_pb2.RelationshipRecord
    else:
        exit(f"unkown schema: {schema}")

    prod = ESProducer(index, topic, schema)
    prod.process()

if __name__ == '__main__':
    main()
