import logging
import os
import sys

from gleif_reader import GleifReader

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

def run():
    args = sys.argv[1:]
    if len(args) != 2:
        exit(f'missing argument: {sys.argv[0]} [type] [input file]')
    typef = args[0]
    path = args[1]

    reader = GleifReader()
    if typef == 'lei':
        reader.readLEI2(path)
    elif typef == 'rr':
        reader.readRR(path)
    else:
        print('unkown type provided: {lei, rr}')

if __name__ == "__main__":
    run()
