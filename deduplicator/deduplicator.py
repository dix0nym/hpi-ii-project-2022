import os
import re
import json
import logging
import hashlib

from utils.es_lib import ESLib
from utils.producer import Producer

from google.protobuf.json_format import Parse, ParseDict,MessageToDict
from build.gen.parsed_hrb.v1 import company_pb2, ceo_relation_pb2, duplicate_ceo_pb2
from build.gen.parsed_hrb.v1.duplicate_ceo_pb2 import duplicate_ceo
from build.gen.parsed_hrb.v1.ceo_relation_pb2 import ceo_relation

logging.basicConfig(
        level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)


log = logging.getLogger(__name__)

def getHash(s):
    lib = hashlib.sha256()
    lib.update(s.encode('utf-8'))
    return lib.hexdigest()

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

class Deduplicator:
    def __init__(self, index):
        self.index = index
        self.es = ESLib(index)
        self.producer = Producer('ceo_relation', ceo_relation, buffered=False)
        self.ceo_producer = Producer('duplicate_ceo', duplicate_ceo)
        self.duplicate_count = 0

    def compositeAggregate(self, after=None):
        data = {
                "query": {
                    "match_all": {}
                },
                "aggs": {
                    "ceos": {
                        "composite": {
                            "size": 1000,
                            "sources": [
                                {"birthdate": {"terms": {"field": "birthdate"}}}
                            ]
                        },
                        "aggregations": {
                            "ceos_firstnames": {
                                "terms": {
                                    "field": "firstname",
                                    "min_doc_count": 2,
                                    "size": 1000
                                }
                            }
                        }
                    }
                },
                "size": 0
            }
        if after:
            data['aggs']['ceos']['composite']['after'] = after
        return self.es.postES(f"/{self.index}/_search", data)


    def produce_ceo(self, ceo, original_id):
        key = ceo['_id']
        conv_obj = convert_item(ceo['_source'])
        obj = ParseDict(conv_obj, duplicate_ceo(), ignore_unknown_fields=True)
        obj.original_id = original_id
        log.info(f'archiving ceo {key} - replace by {original_id}')
        self.ceo_producer.produce(obj, key)

    
    def update_relation(self, relation, ceo_id):
        key = relation['_id']
        conv_obj = convert_item(relation['_source'])
        obj = ParseDict(conv_obj, ceo_relation(), ignore_unknown_fields=True)
        obj.ceo_id = ceo_id
        log.info(f'updating relation {key} - replace {obj.ceo_id} with {ceo_id}')
        self.producer.produce(obj, key)

    def checkDuplicates(self, hits):
        names = {}
        for hit in hits:
            source = hit['_source']
            middlename = source['middlename']
            lastname = source['lastname']
            s = f"{middlename}_{lastname}"
            h = getHash(s)
            if h not in names:
                names[h] = []
            names[h].append(hit)
        for h, duplicates in names.items():
            if len(duplicates) < 2:
                continue
            log.info(f"found {len(duplicates)} duplicates for {duplicates[0]['_source']['firstname']} {duplicates[0]['_source']['middlename']} {duplicates[0]['_source']['lastname']} {duplicates[0]['_source']['birthdate']}")
            original = duplicates[0]
            all_relations = []
            for dup in duplicates[1:]:
                relations = self.get_ceo_relations(dup['_source']['id'])
                log.info(f"found {len(relations)} relations for duplicate {dup['_source']['id']}")
                all_relations.extend(relations)
                for relation in relations:
                    self.update_relation(relation, original['_source']['id'])
                self.produce_ceo(dup, original['_source']['id'])
            self.duplicate_count += len(duplicates) - 1

    
    def get_ceo_relations(self, ceo_id):
        lib = ESLib('ceo_relation')
        (status, page) = lib.search({'match':{'ceo_id': ceo_id}})
        hits = page['hits']['hits']
        all_hits = hits
        scroll_id = page['_scroll_id']
        while True:
            (status, page) = lib.scroll(scroll_id)
            if status != 200:
                break
            if '_scroll_id' in page:
                scroll_id = page['_scroll_id']
            hits = page['hits']['hits']
            if not hits:
                break
            all_hits.extend(hits)
        lib.deleteScroll(scroll_id)
        return all_hits


    def search_key(self, key1, key2, size=10):
        (status, page) = self.es.search({'bool':{'must':[{'term': {'birthdate': key1}}, {'term': {'firstname': key2}}]}}, size=size, sort=[{'_doc': 'desc'}])
        hits = page['hits']['hits']
        scroll_id = page['_scroll_id']
        all_hits = hits
        while True:
            (status, page) = self.es.scroll(scroll_id)
            if status != 200:
                break
            if '_scroll_id' in page:
                scroll_id = page['_scroll_id']
            hits = page['hits']['hits']
            if not hits:
                break
            all_hits.extend(hits)
        self.es.deleteScroll(scroll_id)
        return all_hits

    def handleResults(self, buckets):
        for bucket in buckets:
            doc_count = bucket['doc_count']
            if doc_count < 2:
                continue
            key = bucket['key']['birthdate']
            ceos_firstnames = bucket['ceos_firstnames']['buckets']
            for bucket2 in ceos_firstnames:
                key2 = bucket2['key']
                hits = self.search_key(key, key2)
                log.info(f'found {len(hits)} entries with dob: {key} and firstname: {key2}')
                self.checkDuplicates(hits)

    def start(self):
        after = None
        while True:
            (status, page) = self.compositeAggregate(after=after)
            ceos = page['aggregations']['ceos']
            self.handleResults(ceos['buckets'])
            if 'after_key' in ceos:
                after = page['aggregations']['ceos']['after_key']
            else:
                break
        log.info(f'found {self.duplicate_count} duplicates')

              


if __name__ == '__main__':
   d =  Deduplicator('ceo')
   d.start()
