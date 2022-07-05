import logging
import os
import sys
from utils.producer import Producer
from gf_crawler.gf_reader import GfReader
from build.gen.gleif.v1.lei_pb2 import LEIRecord
from build.gen.gleif.v1.relationship_pb2 import RelationshipRecord

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

def run():
    args = sys.argv[1:]
    if len(args) != 2:
        exit(f'missing argument: {sys.argv[0]} [recordType] [input file]')
    recordType = args[0]
    path = args[1]
    if recordType not in ['lei', 'rr']:
        exit('unkown recordType provided: {lei, rr}')
    topic = 'gf-lei'
    schema = LEIRecord
    if recordType == 'rr':
        schema = RelationshipRecord
        topic = 'gf-relationship'
    producer = Producer(topic, schema)
    reader = GfReader(producer, schema, path)
    if recordType == 'lei':
        reader.readLEI()
    else:
        reader.readRR()

if __name__ == "__main__":
    run()
