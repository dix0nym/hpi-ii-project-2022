from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from build.gen.bakdata.corporate.v1 import corporate_pb2
from build.gen.bakdata.corporate.v1.corporate_pb2 import Corporate, Status
from build.gen.parsed_hrb.v1 import company_pb2, ceo_pb2, address_pb2, owning_company_pb2
from build.gen.parsed_hrb.v1.company_pb2 import Company, CompanyStatus
from build.gen.parsed_hrb.v1.ceo_pb2 import CEO
from build.gen.parsed_hrb.v1.address_pb2 import Address
from build.gen.parsed_hrb.v1.owning_company_pb2 import owning_company

import re
from rb_producer import RbProducer
import logging 
import os
from abc import ABC, abstractmethod
import hashlib

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

company_producer = RbProducer('company')
address_producer = RbProducer('address')
ceo_producer = RbProducer('CEO')
owning_company_producer = RbProducer('owning_company')

def msg_process(msg):
    values = {} 
    values['key'] = msg.key()
    info = msg.value().information
    if msg.value().event_type == 'create': 
        #if msg.value().state == 'bw': 
        parser = NeueintragungsParser(info)
        results = parser.parse()
        company = Company()
        for a in results['parsed_addresse']: 
            extra, strasse, ort = a
            addresse = Address()
            addresse.extra = extra
            addresse.street = strasse
            addresse.city = ort
            addresse.id = hashlib.sha1((extra +  ';' + strasse + ';' + ort).encode()).hexdigest()
            address_producer.produce_to_topic(addresse)
            company.address.append(addresse.id)
        for c in results['ceo']: 
            lastname, firstname, birthplace, birthdate = c
            ceo = CEO()
            ceo.lastname = lastname
            ceo.firstname = firstname
            ceo.birthplace = birthplace 
            ceo.birthdate = birthdate
            ceo.id = hashlib.sha1((lastname +  ';' + firstname + ';' + birthplace + ';' + birthdate).encode()).hexdigest()
            company.ceos.append(ceo.id)
            ceo_producer.produce_to_topic(ceo)
        for o in results['company']: 
            name, ort, hrb = o
            owner = owning_company()
            owner.hrb = hrb
            owner.city = ort
            owner.name = name
            owner.id = hashlib.sha1(hrb.encode()).hexdigest()
            owning_company_producer.produce_to_topic(owner)
            company.owning_company.append(owner.id)
        company.name = results['name']
        company.founding_date = results['parsed_satzung']
        company.company_objective = results['gegenstand']
        company.capital = results['kapital']
        company.state = msg.value().state
        company.reference_id = msg.value().reference_id
        company.id = hashlib.sha1(company.reference_id.encode()).hexdigest()
        company_producer.produce_to_topic(company)
        
class NeueintragungsParser(): 

    def __init__(self, msg):
        self.company = Company() 
        self.results = {'kapital':'', 'bemerkung':'', 'nicht_eingetragen':'', 'vertretung':'', 'rechtsform':'', 'satzung':'', 'register_id':'', 'addresse':[], 'sitz':'', 'firma':'', 'gegenstand':'', 'name':'', 'parsed_addresse' : [], 'ceo' : [], 'company':[], 'parsed_satzung':''}
        self.msg = msg 
        
    def parse_kapital(self):
        start_words = [' Stamm- bzw. Grundkapital', ' Stammkapital', ' Grundkapital', ' Das Stammkapital beträgt', ' Stammkapital nun', ' Kapital', ';Stammkapital', 'Stammkapital', ' Haftungskapital']
        capital = None
        for w in start_words: 
            match = re.search(w + '[:;]*\s?[0-9][0-9,.]*\s[a-zA-Z€]*[\.;,]*', self.msg)
            if match: 
                self.results['kapital'] = self.msg[match.start():match.end()]
                self.msg = self.msg.replace(self.results['kapital'],'$$$cap$$$')

                break
        if capital is None: 
            match = re.search('[\.]\s[0-9][0-9,.]*\s(?:EUR|GBP|CHF|€)[\.;,]*\s', self.msg)
            if match: 
                self.results['kapital'] = self.msg[match.start()+1:match.end()]
                self.msg = self.msg.replace(self.results['kapital'],'$$$cap$$$')
                
    def parse_bemerkung(self): 
        match = re.search('\sBemerkung:[^\$]*', self.msg)
        if match: 
            self.results['bemerkung'] = self.msg[match.start():match.end()]
            self.msg = self.msg.replace(self.results['bemerkung'],'$$$note$$$')
                       
    def parse_nicht_eingetragen(self): 
        start_words = [' Als nicht eingetragen wird bekanntgemacht:', ' Als nicht eingetragen wird bekanntgemacht:', ' Als nicht eingetragen wird veröffentlicht:']
        for w in start_words: 
            match = re.search(w, self.msg)
            if match:
                self.results['nicht_eingetragen'] = self.msg[match.start():]
                self.msg = self.msg.replace(self.results['nicht_eingetragen'],'$$$nicht_eingetragen$$$')
    
    def parse_vertretung(self): 
        vertretung_words = ['Allgemeine Vertretungsregelung:', 'Vertretungsregelung:', 'Vertretungregelung:', 'Ist nur ein', 'Jeder persönlich haftende Gesellschafter', 'Der Inhaber', 'Die Inhaberin', 'Die Gesellschaft hat stets nur einen persönlich haftenden Gesellschafter.', 'Die Gesellschaft hat', 'Die Gesellschaft wird', 'Die Genossenschaft wird', 'Jeder Partner', 'Die Geschäftsführer', 'Jeder Geschäftsführer']
        for w in vertretung_words: 
            match = re.search(w + '[^\$]*?:', self.msg)
            if match:
                tmp = self.msg[match.start():match.end()]
                end = tmp.rfind('.')
                self.results['vertretung'] = self.msg[match.start(): match.start() + end + 1]
                self.msg = self.msg.replace(self.results['vertretung'], '$$$vertretung$$$')
        #if '$$$vertretung$$$' not in self.msg: 
        #    print(self.msg)
        
    def parse_satzung(self): 
        start_words = ['Satzung vom', 'Gesellschaftsvertrag vom', 'Die Satzung ist', 'Musterprotokoll vom', 'Gesellschaftsvertrag vom:']
        for w in start_words: 
            match = re.search(w + '[^\$^:]*?\.(?:\s|$)', self.msg)
            if match: 
                self.results['satzung'] = self.msg[match.start(): match.end()]
                self.msg = self.msg.replace(self.results['satzung'], '$$$satzung$$$')
                m2 = re.search('\d{2}\.\d{2}\.\d{4}', self.results['satzung'])
                if m2: 
                    self.results['parsed_satzung'] = self.results['satzung'][m2.start():m2.end()]
                
    def parse_addresse(self): 
        start_words = ['Geschäftsanschrift:', 'Geschäftsanschrift:;']
        for w in start_words: 
            while True: 
                match = re.search(w + '[^\$^:]*?\d{5}[^\$^:]*?(?:\.|;|\sGegenstand)', self.msg)
                if match:
                    addresse = self.msg[match.start(): match.end()]
                    m2 = re.search('\sGegenstand', addresse)
                    if m2: 
                        addresse = addresse[:m2.start()]
                    self.msg = self.msg.replace(addresse, '$$$addresse$$$')
                    self.results['addresse'].append(addresse)
                else: 
                    break
    def parse_addresse2(self): 
        while True: 
            match = re.search(',\s[^,^\$^:^;^\*^\)]*?,[^,^\$^:^;^\*^\)]*?,[^,^\$^:^;^\*^\)]*?\d{5}[^,^\$^:^;^\*^\)]*?(?<!\si)\.', self.msg)
            if match and '$$$' not in self.msg[:match.start()]:
                name = self.msg[:match.start()]
                addresse = self.msg[match.start(): match.end()]
                self.msg = self.msg.replace(addresse, '$$$addresse$$$')
                self.results['addresse'].append(addresse[2:])
                self.msg = self.msg.replace(name, '$$$name$$$')
                self.results['name'] = name
                parts = addresse.split(',')
                ort = parts[3][:-1]
                strasse = parts[2]
                extra = parts[1]
                self.results['parsed_addresse'].append((extra, strasse, ort))
            else: 
                break
        if self.results['name'] == '': 
            match = re.search('\([^\$\)]*\d{5}[^\$\)]*\)', self.msg)
            if match and '$$$' not in self.msg[:match.start()]:
                name = self.msg[:match.start()]
                addresse = self.msg[match.start():match.end()]
                self.msg = self.msg.replace(addresse, '$$$addresse$$$')
                self.results['addresse'].append(addresse[1:-1])
                self.msg = self.msg.replace(name, '$$$name$$$')
                self.results['name'] = name
                parts = addresse[1:-1].split(',')
                extra = ''
                strasse = '' 
                ort = ''
                if len(parts) == 1: 
                    ort = parts[0].strip()
                elif len(parts) == 2: 
                    strasse = parts[0]
                    ort = parts[1].strip()
                elif len(parts) == 3: 
                    match = re.search('\d{5}', parts[1])
                    if match: 
                        ort = parts[1].strip()
                        strasse = parts[0]
                        self.results['gegenstand'] = parts[2].strip()
                    elif 'c/o' in parts[0]: 
                        extra = parts[0]
                        ort = parts[2].strip()
                        strasse = parts[1]
                    elif 'c/o' in parts[1]: 
                        strasse = parts[0]
                        ort = parts[2].strip()
                        extra = parts[1].strip()
                    else: 
                        extra = parts[0]
                        strasse = parts[1].strip()
                        ort = parts[2].strip()
                self.results['parsed_addresse'].append((extra, strasse, ort))
                
        if self.results['name'] == '': 
            match = re.search(',', self.msg)
            if match: 
                name = self.msg[:match.start()]
                m2 = re.search('\$', self.msg)
                if m2: 
                    addresse = self.msg[match.start()+1:m2.start()]
                    self.msg = self.msg.replace(addresse, '$$$addresse$$$')
                    self.results['addresse'].append(addresse)
                self.msg = self.msg.replace(name, '$$$name$$$')
                self.results['name'] = name
                
    def parse_register_id(self): 
        match = re.search('^[A-Z]+\s\d+(?:\s[A-Z]*)?:', self.msg)
        if match: 
            self.results['register_id'] = self.msg[match.start():match.end()+1]
            self.msg = self.msg.replace(self.results['register_id'], '')
        
    def parse_rechtsform(self): 
        match = re.search('Rechtsform:[^\$]*?(?:;|$|\.\s)', self.msg)
        if match: 
            self.results['rechtsform'] = self.msg[match.start():match.end()]
            self.msg = self.msg.replace(self.results['rechtsform'], '$$$rechtsform$$$')
            
    def parse_sitz(self): 
        start_words = ['Sitz:', 'Sitz / Zweigniederlassung:', 'Sitz der Firma:']
        for w in start_words: 
            match = re.search(w + u'\s[A-Za-zäÄöÖüÜ]*', self.msg)
            if match: 
                self.results['sitz'] = self.msg[match.start():match.end()+1]
                self.msg = self.msg.replace(self.results['sitz'], '$$$sitz$$$')
        
    def parse_gegenstand(self): 
        start_words =  ['\(Unternehmensgegenstand:', 'Gegenstand:', 'Gegenstand des Unternehmens', 'Gegenstand der Zweig', 'Unternehmensgegenstand ist', 'Unternehmensgegenstand:']
        for w in start_words: 
            match = re.search(w + '[^\$]*(?<!Co)\.', self.msg) 
            if match: 
                self.results['gegenstand'] = self.msg[match.start():match.end()]
                self.msg = self.msg.replace(self.results['gegenstand'], '$$$gegenstand$$$')                
            
    def parse_firma(self): 
        start_words = ['Firma:']
        for w in start_words: 
            match = re.search(w + '.*?(?:\$|:)', self.msg)
            if match: 
                self.results['firma'] = self.msg[match.start():match.end()-1]
                self.msg = self.msg.replace(self.results['firma'], '$$$firma$$$')
    
    def parse_ceo(self): 
        while True: 
            match = re.search('[^:^\$^,^;]*,[^:^\$^,^;]*,[^:^\$^,^;]*,\s\*\d{2}\.\d{2}\.\d{4}', self.msg)
            if match: 
                ceo = self.msg[match.start():match.end()]
                self.msg = self.msg.replace(ceo, '$$$ceo$$$')
                parts = ceo.split(',')
                lastname = parts[0].replace('1.', '').strip()
                firstname = parts[1].strip()
                birthplace = parts[2].strip()
                birthdate = parts[3][2:]
                self.results['ceo'].append((lastname, firstname, birthplace, birthdate))
            else: 
                break
        while True: 
            match = re.search('[^:^\$^,^;]*,[^:^\$^,^;]*,\s\*\d{2}\.\d{2}\.\d{4},\s[^:^\$^,^;^\s]*', self.msg)
            if match: 
                ceo = self.msg[match.start():match.end()]
                self.msg = self.msg.replace(ceo, '$$$ceo$$$')
                parts = ceo.split(',')
                lastname = parts[0].replace('1.', '').strip()
                firstname = parts[1].strip()
                birthplace = parts[3].strip()
                birthdate = parts[2][2:]
                self.results['ceo'].append((lastname, firstname, birthplace, birthdate))
            else: 
                break
        while True: 
            match = re.search('[^:^\$^,^;]*,[^:^\$^,^;]*,\s\*\d{2}\.\d{2}\.\d{4}', self.msg)
            if match: 
                ceo = self.msg[match.start():match.end()]
                self.msg = self.msg.replace(ceo, '$$$ceo$$$')
                parts = ceo.split(',')
                lastname = parts[0].replace('1.', '').strip()
                firstname = parts[1].strip()
                birthplace = ''
                birthdate = parts[2][2:]
                self.results['ceo'].append((lastname, firstname, birthplace, birthdate))
            else: 
                break
    
    def parse_company_owner(self): 
        while True: 
            match = re.search('[^:^\$^;]*?[A-Za-zäÄüÜöÖ],\s[A-Za-zäÄüÜöÖ]*\s\([^:^\$^;]*?HRB[^:^\$^;]*?\)', self.msg)
            if match: 
                company = self.msg[match.start():match.end()]
                self.msg = self.msg.replace(company, '$$$company$$$')
                company = company.replace('1.', '')
                parts = company.split(',')
                name = parts[0].strip()
                ort = parts[1].split('(')[0].strip() 
                m2 = re.search('HRB.*?\)', company)
                if not m2: 
                    print(company)
                hrb = company[m2.start():m2.end()-1]
                self.results['company'].append((name, ort, hrb))
            else: 
                break
        #if 'HRB' in self.msg: 
        #    print(self.msg)
        
    def parse(self): 
        #if 'TREGAL' in self.msg: 
        #    print(self.msg)
        #parts = self.msg.split(':')
        #for p in parts: 
        #    print(p[-25:])
        self.parse_nicht_eingetragen()
        self.parse_bemerkung()
        self.parse_kapital()
        self.parse_rechtsform()
        self.parse_vertretung()
        self.parse_satzung()
        self.parse_register_id()
        self.parse_addresse()
        self.parse_sitz()
        self.parse_firma()
        self.parse_gegenstand()
        self.parse_addresse2()
        self.parse_ceo()
        self.parse_company_owner()
        #print(self.msg)
        #if 'Sitz:' in self.msg: 
        #    print(self.msg)   
        #i = self.msg.rfind(':')
        #print(self.msg[i-20:])
        #if self.results['name'] == '': 
        #    print(self.msg)
        #    print(self.results)
        #print(self.results['satzung'])
        return self.results
        
from rb_crawler.constant import SCHEMA_REGISTRY_URL, BOOTSTRAP_SERVER, TOPIC
protobuf_deserializer = ProtobufDeserializer(corporate_pb2.Corporate, {"use.deprecated.format": True})
        
conf = {'bootstrap.servers': BOOTSTRAP_SERVER, 'group.id': 'kafka_connect', 'enable.auto.commit': False,
        'auto.offset.reset': 'earliest', 'key.deserializer' : StringDeserializer('utf_8'), 'value.deserializer': protobuf_deserializer}

consumer = DeserializingConsumer(conf)

topics = [TOPIC]

def consume_loop(consumer, topics): 
    try:
        consumer.subscribe(topics)
        print(f'subscribed to {topics}')
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: 
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

consume_loop(consumer, topics)
