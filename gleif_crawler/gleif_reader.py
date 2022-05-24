import json
import logging
import operator
from functools import reduce

from gleif_producer import GleifProducer
import lxml.etree as ET
import xmltodict
from google.protobuf.json_format import Parse, ParseDict

from gleif.v1 import gleif_pb2
from gleif.v1.gleif_pb2 import LEIRecord
from gleif.v1.relationship_pb2 import RelationshipRecord

log = logging.getLogger(__name__)

class GleifReader:
    def __init__(self):
        self.producer = GleifProducer()
    
    def getKey(self, record, path, default=None):
        try:
            return reduce(operator.getitem, path, record)
        except (KeyError, TypeError):
            return default

    def setKey(self, record, path, value):
        self.getKey(record, path[:-1])[path[-1]] = value
    

    def get_tag(self, element):
        # get rid of namespaces before tag name - e.g. lei:LEIRecord
        return ET.QName(element.tag).localname
    
    def fix_address(self, record, path):
        # hack to fix xml with duplicate keys (2221009B7L3UTZMC9I66) in AdditonalAddressLine
        address = self.getKey(record, path)
        if address:
            if isinstance(address, list):
                self.setKey(record, path, "; ".join(address))

    def fix_legalname(self, record):
        # hack to fix error in LegalName - xmltodict doesn't make a string from value because of attribute
        # "LegalName": {"@http://www.w3.org/XML/1998/namespace:lang": "en", "@xmlns": {"gleif": "http://www.gleif.org/concatenated-file/header-extension/2.0", "lei": "http://www.gleif.org/data/schema/leidata/2016"}, "#text": "AFRINVEST SECURITIES LIMITED"}
        # so just get rid of it - record['Entity']['LegalName'] = record['Entity']['LegalName']['#text']
        if '#text' in record['Entity']['LegalName']:
            self.setKey(record, ['Entity', 'LegalName'], self.getKey(record, ['Entity', 'LegalName', '#text']))

    def handle_record(self, xml_record):
        record = xmltodict.parse(ET.tostring(xml_record), process_namespaces=True, namespaces={'http://www.gleif.org/data/schema/leidata/2016': None})
        if not 'LEIRecord' in record:
            return
        record = record['LEIRecord']
        # hacks to fix dataset
        self.fix_legalname(record)
        self.fix_address(record, ['Entity', 'HeadquartersAddress', 'AdditionalAddressLine'])
        self.fix_address(record, ['Entity', 'LegalAddress', 'AdditionalAddressLine'])

        json_data = json.dumps(record)
        message = Parse(json_data, LEIRecord(), ignore_unknown_fields=True)
        self.producer.produce_to_topic(message)

    
    def writeFile(self, name, lines):
        with open(f"{name}.txt", 'w+') as f:
            for line in lines:
                f.write(f"{line}\n")

    def simplify(self, record, path):
        val = self.getKey(record, path)
        if not val:
            return
        if isinstance(val, list):
            self.setKey(record, path[:-1], val)
        else:
            self.setKey(record, path[:-1], [val])

    def simplify_all(self, record):
        paths = [
            ['Relationship', 'RelationshipQualifiers', 'RelationshipQualifier'],
            ['Relationship', 'RelationshipQuantifiers', 'RelationshipQuantifier'],
            ['Relationship', 'RelationshipPeriods', 'RelationshipPeriod']
        ]
        for path in paths:
            self.simplify(record, path)
    
    def clean_name(self, record, path):
        val = self.getKey(record, path)
        if not val:
            return
        self.setKey(record, path, val.replace('-', '_'))

    def handle_relationship(self, xml_relationship):
        relationship = xmltodict.parse(ET.tostring(xml_relationship), process_namespaces=True, namespaces={'http://www.gleif.org/data/schema/rr/2016': None})
        if not 'RelationshipRecord' in relationship:
            print('early return')
            return
        relationship = relationship['RelationshipRecord']
        # simplify data structure
        self.simplify_all(relationship)
        self.clean_name(relationship, ['Relationship', 'RelationshipType'])
        
        json_data = json.dumps(relationship)
        message = Parse(json_data, RelationshipRecord(), ignore_unknown_fields=True)
        self.producer.produce_to_topic(message)
    
    def read(self, path, tag, callback=print):
        doc = ET.iterparse(path, events=('end',))
        for event, element in doc:
            if event == 'end' and self.get_tag(element) == tag:
                callback(element)
                element.clear()

    def readLEI2(self, path):
        self.read(path, 'LEIRecord', self.handle_record)

    def readRR(self, path):
        self.read(path, 'RelationshipRecord', self.handle_relationship)
