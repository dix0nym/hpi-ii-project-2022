from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from build.gen.bakdata.corporate.v1 import corporate_pb2
from build.gen.bakdata.corporate.v1.corporate_pb2 import Corporate, Status, Company, Address, CEO
import re
from rb_producer import RbProducer
import logging 
import os

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

producer = RbProducer()

def msg_process(msg):
    values = {} 
    values['key'] = msg.key()
    info = msg.value().information
    if msg.value().event_type == 'create': 
        entry = handle_neueintragung(msg)
        producer.produce_to_topic(company=entry)
    elif msg.value().event_type == 'update':
        entry = handle_update(msg)
        producer.produce_to_topic(company=entry)
    elif msg.value().event_type == 'delete':
        entry = handle_delete(msg)
        producer.produce_to_topic(company=entry)        
    
rechtsformen = ['GmbH', 'mbH', 'UG (haftungsbeschränkt)', 'e.K.', 'e.V.', 'Limited', 'm.b.H', 'Verwaltungsgesellschaft', 'Ltd', 'GmbH.']
ceo_words = ['Geschäftsführer: ', 'director: ', 'Inhaber: ', 'Geschäftsführerin: ', 'Persönlich haftender Gesellschafter: ', 'Persönlich haftende Gesellschafterin: ', 'Vorstand: ', 'Partner: ', 'Director und ständiger Vertreter, von den Beschränkungen des § 181 BGB befreit mit der Befugnis, allein zu vertreten: ', 'Director und ständiger Vertreter mit der Befugnis, allein zu vertreten: ', 'Director: ', 'Geschäftsführer:; ', 'Persönlich haftender Gesellschafter:; ']
def handle_neueintragung(msg):
    values = msg.value()
    entry = Company() 
    entry.id = values.id
    entry.rb_id = values.rb_id 
    entry.state = values.state
    entry.reference_id = values.reference_id
    entry.event_date = values.event_date 
    entry.event_type = 'Neueintragungen'
    entry.status = Status.STATUS_ACTIVE
    info = values.information
    entry.info = info

    parse_info(entry, info)
    return entry
    
def parse_info(entry, info):    
    # nicht eingetragen 
    nicht_eingetragen_i = info.find('Als nicht eingetragen wird veröffentlicht')
    nicht_eingetragen_string = None
    if nicht_eingetragen_i != -1: 
        nicht_eingetragen_string = info[nicht_eingetragen_i:]
        info = info[:nicht_eingetragen_i]
        
    nicht_eingetragen_i = info.find('Nicht eingetragen')
    if nicht_eingetragen_i != -1: 
        nicht_eingetragen_string = info[nicht_eingetragen_i:]
        info = info[:nicht_eingetragen_i]
    
        
    nicht_eingetragen_i = info.find('Als nicht eingetragen wird noch veröffentlicht')
    if nicht_eingetragen_i != -1: 
        nicht_eingetragen_string = info[nicht_eingetragen_i:]
        info = info[:nicht_eingetragen_i]
        
    nicht_eingetragen_i = info.find('Als nicht eingetragen wird bekanntgemacht')
    if nicht_eingetragen_i != -1: 
        nicht_eingetragen_string = info[nicht_eingetragen_i:]
        info = info[:nicht_eingetragen_i]
    
    # detect rechtsverhältnis 
    rechtsverhaltnis_i = info.find('Rechtsverhaeltnis')
    rechtsverhaltnis_string = None
    if rechtsverhaltnis_i != -1: 
        rechtsverhaltnis_string = info[rechtsverhaltnis_i:]
        info = info[:rechtsverhaltnis_i]
    
    tokens = {}
    tokens[1] = info
    
    vertrag_date_string = '' 
    # detect rechtsform
    rechtsform_i = info.find('Rechtsform: ')
    rechtsform_string = None
    if rechtsform_i != -1: 
        #print(info[rechtsform_i:])
        end = info[rechtsform_i:].find(';')
        if end == -1: 
            rechtsform_string = info[rechtsform_i:]
            tokens[1] = info[:rechtsform_i]
        else: 
            rechtsform_string = info[rechtsform_i:rechtsform_i+end]
            tokens[1] = info[:rechtsform_i]
            vertrag_date_string = info[rechtsform_i+end+2:]
    
    extra_info = ''
    vertrag_date_i = -1

    vertrag_date_i = tokens[1].find('Gesellschaftsvertrag vom: ')
    if vertrag_date_i != -1: 
        vertrag_date_string = tokens[1][vertrag_date_i:]
        tokens[1] = tokens[1][:vertrag_date_i]

    gegenstand_words = ['Gegenstand:', 'Gegenstand des Unternehmens', 'Gegenstand der Zweig', 'Unternehmensgegenstand ist']
    gegenstand = '' 
    for s in gegenstand_words: 
        gegenstand_i = tokens[1].find(s)
        if gegenstand_i != -1: 
           end = tokens[1][gegenstand_i:].find('. ')
           if tokens[1][gegenstand_i:][end-2:end] == 'Co': 
               end = end + tokens[1][gegenstand_i+end+1:].find('. ') + 1
           if end != -1: 
               problem_i = tokens[1][gegenstand_i:gegenstand_i+end+1].find('Stammkapital')
               if problem_i != -1: 
                   alt_end = tokens[1][gegenstand_i:gegenstand_i+problem_i].rfind(' ')
                   if alt_end != -1: 
                       end = alt_end
               gegenstand = tokens[1][gegenstand_i:gegenstand_i+end+1]
               tokens[2] = tokens[1][gegenstand_i+end+1:]
               tokens[1] = tokens[1][:gegenstand_i]
               break
               
    capital_words = ['Stamm- bzw. Grundkapital: ', 'Stammkapital:', 'Stammkapital ', 'Grundkapital: ', 'Grundkapital;', 'Stammkapital;', 'Stammkapital:;']
    capital = ''
    for s in capital_words: 
        if 2 not in tokens.keys(): 
            break
        stammkapital_i = tokens[2].find(s)
        if stammkapital_i != -1: 
            amount = re.search(s + '\s?[0-9,.]+\s.*?\s', tokens[2])
            if amount:
                capital = tokens[2][amount.start():amount.end()]
                gegenstand += tokens[2][:amount.start()]
                tokens[2] = tokens[2][amount.end():]
                break

    
    sitz = ''
    firma = ''
    if 2 in tokens.keys() and 'Sitz: ' in tokens[2]: 
        sitz_i = tokens[2].find('Sitz: ')
        end = tokens[2][sitz_i + len('Sitz: '):].find(':')
        if end != -1: 
            end += len('Sitz: ')
            alt_end = tokens[2][sitz_i:sitz_i+end].find('.')
            if alt_end != -1: 
                end = alt_end 
            else: 
                end = tokens[2][sitz_i + len('Sitz: ') + 1:sitz_i+end].find(' ') + len('Sitz: ') + 1
        sitz = tokens[2][sitz_i:sitz_i + end]
        firma_i = tokens[2][:sitz_i].find('Firma:')
        if firma_i != -1: 
            firma = tokens[2][firma_i:sitz_i]
            gegenstand += tokens[2][:firma_i]
        tokens[2] = tokens[2][sitz_i + end:]             
       
    if 2 in tokens.keys():
        vertretung_i = tokens[2].find('sregelung: ')
        if vertretung_i != -1:
            tokens[2] = tokens[2][vertretung_i + len('sregelung:'):]
       
    
    ceo_rights_2_string = []  
    ceo_list = []
    if 2 in tokens.keys():
        tokens[2] = tokens[2].replace('Bestellt: ','')
        tokens[2] = tokens[2].replace('Ausgeschieden: ','Ausgeschieden')
        ceo_parts = tokens[2].split(':')
        if len(ceo_parts) > 0: 
            i = ceo_parts[0].rfind('.')
            alt_i = ceo_parts[0].rfind(';')
            if alt_i > i: 
                i = alt_i
            if i != -1: 
                ceo_rights_2_string.append(ceo_parts[0][:i])
            for j in range(len(ceo_parts) - 1):
                j += 1
                title = None
                if i == -1: 
                    title = ceo_parts[j-1]
                else: 
                    title = ceo_parts[j-1][i+2:]
                
                i = ceo_parts[j].rfind('.')
                alt_i = ceo_parts[j].rfind(';')
                if alt_i > i: 
                    i = alt_i
                if i != -1:
                    ceo_list.append((title, ceo_parts[j][:i]))
                else:
                    ceo_list.append((title, ceo_parts[j])) 
    
    gesellschaftsvertrag_words = ['Satzung vom', 'Gesellschaftsvertrag vom']
    for s in gesellschaftsvertrag_words: 
        vertrag_i = tokens[1].find(s)
        if vertrag_i != -1: 
            end = tokens[1][vertrag_i:].find('. ')
            if end != -1:
                vertrag_date_string = tokens[1][vertrag_i:vertrag_i+end]
                tokens[1] = tokens[1][:vertrag_i]
                break

    
    i = tokens[1].find('Vertretungsregelung:') 
    if i != -1: 
        tokens[3] = tokens[1][i+len('Vertretungsregelung:') +1:]
        tokens[1] = tokens[1][:i]
        
    if 3 in tokens.keys():
        tokens[3] = tokens[3].replace('Bestellt: ','')
        tokens[3] = tokens[3].replace('Ausgeschieden: ','Ausgeschieden')
        ceo_parts = tokens[3].split(':')
        if len(ceo_parts) > 0: 
            i = ceo_parts[0].rfind('.')
            alt_i = ceo_parts[0].rfind(';')
            if alt_i > i: 
                i = alt_i
            if i != -1: 
                ceo_rights_2_string.append(ceo_parts[0][:i])
            for j in range(len(ceo_parts) - 1):
                j += 1
                title = None
                if i == -1: 
                    title = ceo_parts[j-1]
                else: 
                    title = ceo_parts[j-1][i+2:]
                i = ceo_parts[j].rfind('.')
                alt_i = ceo_parts[j].rfind(';')
                if alt_i > i: 
                    i = alt_i
                if i != -1:
                    ceo_list.append((title, ceo_parts[j][:i]))
                else:
                    ceo_list.append((title, ceo_parts[j])) 
                    
    tokens[1] = tokens[1].replace('geändert:', 'geändert')
    if 'Sitz: ' in tokens[1]: 
        sitz_i = tokens[1].find('Sitz: ')
        end = tokens[1][sitz_i + len('Sitz: '):].find(':')
        if end != -1: 
            end += len('Sitz: ')
            alt_end = tokens[1][sitz_i:sitz_i+end].find('.')
            if alt_end != -1: 
                end = alt_end 
            else: 
                end = tokens[1][sitz_i + len('Sitz: ') + 1:sitz_i+end].find(' ') + len('Sitz: ') + 1
        else: 
            end = tokens[1][sitz_i + len('Sitz: '):].find(';') 
            if end != -1: 
                end += len('Sitz: ')
        if end != -1: 
            sitz = tokens[1][sitz_i:sitz_i + end]
            # some loss here print(tokens[1][sitz_i + end:])
        else: 
            sitz = tokens[1][sitz_i:]
            #tokens[1] = ''
        #print(sitz)
        firma_i = tokens[1][:sitz_i].find('Firma:')
        if firma_i != -1: 
            firma = tokens[1][firma_i:sitz_i]
            #gegenstand += tokens[1][:firma_i]
            tokens[1] = tokens[1][:firma_i]
        else:
            tokens[1] = tokens[1][:sitz_i]
    #if 'kapital' in tokens[1]: 
    #    print(tokens[1])
    name_address = tokens[1]
    name = ''
    ort = ''
    plz = ''
    strasse = ''
    name_match = re.search('\([^\)]*?\d{5}.*?', name_address)
    if name_match: 
        #print(name_address[name_match.start(): name_match.end()])
        plz = name_address[name_match.end()-5: name_match.end()]
        strasse = name_address[name_match.start()+1:name_match.end()-7]
        #print(strasse)
        i = name_address[:name_match.start()].rfind(',')
        if i != -1: 
            name = name_address[:name_match.start()][:i]
            ort = name_address[:name_match.start()][i+2:]
        else: 
            name = name_address[:name_match.start()]
    else: 
        name_split = name_address.split('(')
        if len(name_split) > 0: 
            i = name_split[0].rfind(',')
            if i != -1: 
                name = name_split[0][:i]
                ort = name_split[0][i+2:]
            else: 
                name = name_split[0]
            
        if len(name_split) > 1: 
            i = name_split[1].find(')')
            if i != -1: 
                strasse = name_split[1][:i]
            else: 
                strasse = name_split[1]

    entry.address.plz = plz
    entry.address.city_name = ort
    entry.address.street = strasse 
    entry.name = name
    
    for ceo_title, ceo_name in ceo_list:
        ceo_candidates = [ceo_name]
        changed = False
        while(True):
            changed = False
            for c in ceo_candidates: 
                matches = [', jeweils einzelvertretungsberechtigt;.*?abzuschließen', ', einzelvertretungsberechtigt; mit.*?abzuschließen', ', mit der Befugnis.*?abzuschließen', ', einzelvertretungsberechtigt', ', jeweils einzelvertretungsberechtigt', '; mit der Befugnis.*?abzuschließen', ';', 'Befugnis die.*?vertreten']
                for m in matches:
                    match = re.search(m, c)
                    if match: 
                       ceo_candidates.remove(c)
                       ceo_candidates.append(c[:match.start()])
                       ceo_candidates.append(c[match.end():])
                       changed = True
                       break
            if not changed:
                break 
        while True:
            changed = False 
            for c in ceo_candidates:
                ceo_match = re.search('[A-Z][^0-9]*?,\s\*\d{2}\.\d{2}\.\d{4}', c)
                if ceo_match: 
                    bday = c[ceo_match.end()-10:ceo_match.end()]
                    n = c[ceo_match.start():ceo_match.end()-13]
                    changes = True
                    ceo_candidates.remove(c)
                    ceo_candidates.append(c[:ceo_match.start()])
                    ceo_candidates.append(c[ceo_match.end():])
                    ceo = CEO() 
                    ceo.title = ceo_title
                    ceo.name = n
                    ceo.birthdate = bday
                    ceo.company = False
                    entry.ceos.append(ceo)
                    #print(c[ceo_match.start():ceo_match.end()])
                else:
                    ceo_match = re.search('^.*?[A-Za-z],\s[A-Za-z]*\s\(.*?HRB.*?\)', c)
                    if ceo_match: 
                        tmp = c[ceo_match.start():ceo_match.end()]
                        sec_match = re.search('\(.*?HRB.*?\)', tmp)
                        hrbid = tmp[sec_match.start()+1: sec_match.end()-1]
                        n = tmp[:sec_match.start()]
                        changes = True
                        ceo_candidates.remove(c)
                        ceo_candidates.append(c[:ceo_match.start()])
                        ceo_candidates.append(c[ceo_match.end():])
                        ceo = CEO() 
                        ceo.title = ceo_title
                        ceo.name = n 
                        ceo.company = True
                        ceo.hrb_number = hrbid
                        entry.ceos.append(ceo)
                    
            if not changed: 
               break
    entry.founding_date = vertrag_date_string
    num_match = re.search('\d.*[A-Za-z]', capital)
    if num_match: 
        cap = capital[num_match.start():num_match.end()]
        entry.capital = cap 

    entry.company_objective = gegenstand
    return entry
    #producer.produce_to_topic(corporate=corporate)
        #while(True): 
        #    ceo_match = re.search('[^;]*?,[^;]*?,[^;]*?,\s\*\d{2}\.\d{2}\.\d{4}', ceo_name)
            #if ceo_match: 
            #    print(ceo_name[ceo_match.start(): ceo_match.end()])
        #    break
        #print(ceo_name)
    
        #else: 
        #    print(name_split)
        #else: 
        #    print(name_split[0])
        #print(name_split[0])
            #print(tokens[1][i - 20:i])
    #print(vertrag_date_string)
    #vertretungs_strings = ['Allgemeine Vertretungsregelung:']
    #for s in vertretungs_strings: 
    #    if s in tokens[1]: 
    #        string_index = tokens[1].find(s)
    #        end_index = tokens[1][string_index:].find('vertreten.') 
    #        if end_index == -1: 
    #            end_index = tokens[1][string_index:].find('Gesellschaft.')
    #            if end_index == -1: 
    #                end_index = tokens[1][string_index:].find('gemeinsam.')
    #                if end_index == -1: 
    #                    end_index = tokens[1][string_index:].find('erteilt werden.')
    #                    if end_index == -1: 
    #                        end_index = tokens[1][string_index:].find('Prokuristen.')
    #                        if end_index != -1: 
    #                             end_index += len('Prokuristen.')
    #                    else:
    #                        end_index += len('erteilt werden.')
    #                else:
    #                    end_index += len('gemeinsam.') 
    #            else:
    #                end_index += len('Gesellschaft.') 
    #        
    #        else: 
    #            end_index += 10
    #        tokens[3] = tokens[1][string_index + end_index:]
    #        ceo_rights_2_string.append(tokens[1][string_index:string_index + end_index])
    #        tokens[1] = tokens[1][:string_index]
    
    #ceo_rights_2 = re.search('einzelvertretungsberechtigt;'[::-1] + '.*?,', tokens[1][::-1])
    #if ceo_rights_2 != None:
    #    dot_i = info[len(info) - ceo_rights_2.end():].find('.')
    #    ceo_rights_2_string.append(tokens[1][len(tokens[1]) - ceo_rights_2.end()+2:len(tokens[1]) - ceo_rights_2.end() + dot_i + 1])
    #    tokens[3] = tokens[1][len(tokens[1]) - ceo_rights_2.end() + dot_i + 1:]
    #    tokens[1] = tokens[1][:len(info) - ceo_rights_2.end()]
    # 
    #ceo_rights_2_i = info.find(', mit der Befugnis')
    #if ceo_rights_2_i != -1: 
    #    if ':' not in info[ceo_rights_2_i:]: 
    #        ceo_rights_2_string.append(info[ceo_rights_2_i:][2:])
    #        info = info[:ceo_rights_2_i]
    #print(ceo_rights_2_string)
    
    
    # former directors 
    #directors = []
    #former_directors = []
    #    
    #while True: 
    #    former_directors_i = info.rfind('Ausgeschieden:')
    #    if former_directors_i == -1: 
    #        break
    #    directors_i = info[former_directors_i:].rfind('Bestellt: ')
    #    if directors_i != -1:
    #        directors.append(info[former_directors_i + directors_i:])
    #        info = info[:former_directors_i + directors_i]
    #    former_directors.append(info[former_directors_i:])
        #print(info[former_directors_i:])
    #    info = info[:former_directors_i]
    
    #if 'abzuschließen.' in info: 
    #    print(info)
    #print(info[-40:])

           
    
    # detect ceos with birthdate
    #ceos_by_date = re.findall('\d{4}\.\d{2}\.\d{2}\*.*?:.*?(?:\.|;)', info[::-1])
    #for c in ceos_by_date: 
    #    ceo_string = c[::-1][2:]
        #print(ceo_string)
        #if 'War' in ceo_string: 
        #   print(info)
    #if len(ceos_by_date) == 0: 
        # detect ceos with HRB
     #   ceos_by_hrb = re.findall('\)(?:[A-Z]\s)?\d*\s{1}BRH.*?:.*?(?:\.|;)', info[::-1])
     #   for c in ceos_by_hrb: 
     #       ceos_by_hrb_min = re.findall('(?:\.|;).*?:.*?HRB\s{1}\d*(?:\s[A-Z])?\)', c[::-1])
     #       for c in ceos_by_hrb_min: 
     #           ceo_string = c[2:]
                #print(ceo_string)
            #if len(ceos_by_hrb_min) == 0: 
                #print(info)
        #if len(ceos_by_hrb) == 0: 
        #    print(info)
    #ceo info
    #ceo_split = None
    #for ceow in ceo_words: 
   # 	if ceow in info: 
   # 	   ceo_split = info.split(ceow)
    	   
    #if ceo_split is None: 
    #    print(info)
        
        
    #company name 
    #matches = re.findall(',[^,]*?\(.*?\s{1}\d{5}\s{1}.*?\.', info)
    #if len(matches) > 0:
    #    i = info.find(matches[0]) 
    #    entry.name = info[:i]
    #else: 
    #    entry.name = info.split(',')[0]

def handle_update(msg): 
    values = msg.value()
    entry = Company() 
    entry.id = values.id
    entry.rb_id = values.rb_id 
    entry.state = values.state
    entry.reference_id = values.reference_id
    entry.event_date = values.event_date 
    entry.event_type = 'Veränderungen'
    entry.status = Status.STATUS_ACTIVE
    info = values.information
    entry.info = info
    
    parse_info(entry, info)
    return entry
    
def handle_delete(msg):
    values = msg.value()
    entry = Company() 
    entry.id = values.id
    entry.rb_id = values.rb_id 
    entry.state = values.state
    entry.reference_id = values.reference_id
    entry.event_date = values.event_date 
    entry.event_type = 'Löschungen'
    entry.status = Status.STATUS_INACTIVE
    info = values.information
    entry.info = info
    
    parse_info(entry, info)
    return entry
    
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
