import logging
import os
import re
import sys

from build.gen.bakdata.corporate.v1 import corporate_pb2
from build.gen.bakdata.corporate.v1.corporate_pb2 import Corporate
from build.gen.gleif.v1 import gleif_pb2
from build.gen.gleif.v1 import relationship_pb2
from build.gen.gleif.v1.gleif_pb2 import LEIRecord
from build.gen.gleif.v1.relationship_pb2 import RelationshipRecord
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import StringDeserializer
from google.protobuf.json_format import ParseDict
from utils.es_lib import ESLib
from utils.producer import Producer


logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

def convert_item(obj):
    pattern = r'^_.*_\d$'
    for k, v in obj.copy().items():
        if isinstance(v, dict):
            obj[k] = convert_item(v)
        if re.match(pattern, k) and isinstance(v, dict):
            for k2, v2 in obj[k].items():
                obj[k2] = v2
            del obj[k]
    return obj

def process(es, producer, schema, size=10):
    (status, page) = es.search({"match_all": {}}, size=size)
    hits = page['hits']['hits']
    scroll_id = page['_scroll_id']
    while len(hits) and status == 200:
        (status, page) = es.scroll(scroll_id)
        if '_scroll_id' in page:
            scroll_id = page['_scroll_id']
        hits = page['hits']['hits']
        for hit in hits:
            assert '_id' in hit and hit['_id']
            conv_obj = convert_item(hit['_source'])
            obj = ParseDict(conv_obj, schema(), ignore_unknown_fields=True)
            producer.produce(obj, hit['_id'])
    producer.poll()
    es.deleteScroll(scroll_id)

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

    producer = Producer(topic, schema)
    es = ESLib(index)
    process(es, producer, schema)

if __name__ == '__main__':
    main()
