import json
import re
import requests
import logging
from google.protobuf.json_format import Parse, ParseDict

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import StringSerializer
from datetime import datetime, timezone

log = logging.getLogger(__name__)

BOOTSTRAP_SERVER: str = "localhost:29092"
SCHEMA_REGISTRY_URL: str = "http://localhost:8081"

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

class ESProducer:
    def __init__(self, index, topic, schema):
        self.host = "http://localhost:9200"
        self.index = index
        self.session = requests.Session()
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
    
    def requestES(self, methode, path, data):
        url = f"{self.host}{path}"
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        request = requests.Request(methode, url, json=data, headers=headers)
        prep_request = self.session.prepare_request(request)
        response = self.session.send(prep_request)
        return (response.status_code, response.json())
        
    def getES(self, path, data):
        return self.requestES('GET', path, data)
    
    def postES(self, path, data):
        return self.requestES('POST', path, data)

    def deleteES(self, path, data):
        return self.requestES('DELETE', path, data)

    def deleteScroll(self, scroll_id):
        return self.deleteES("/_search/scroll", {'scroll_id': [scroll_id]})
    
    def search(self, query, size=10):
        data = {'size': size, 'query': query, 'sort': ["_doc"]}
        return self.postES(f"/{self.index}/_search?scroll=1m", data)

    def scroll(self, scroll_id, keep_alive='1m'):
        data = {'scroll_id': scroll_id, 'scroll': keep_alive}
        return self.postES(f"/_search/scroll", data)

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

    def process(self, size=10):
        (status, page) = self.search({"match_all": {}}, size=size)
        hits = page['hits']['hits']
        scroll_id = page['_scroll_id']
        while len(hits) and status == 200:
            (status, page) = self.scroll(scroll_id)
            if '_scroll_id' in page:
                scroll_id = page['_scroll_id']
            hits = page['hits']['hits']
            for hit in hits:
                assert '_id' in hit and hit['_id']
                conv_obj = convert_item(hit['_source'])
                obj = ParseDict(conv_obj, self.schema(), ignore_unknown_fields=True)
                self.produce(obj, hit['_id'])
        self.poll()
        self.deleteScroll(scroll_id)
        
