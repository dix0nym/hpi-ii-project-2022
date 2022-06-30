import logging
import os

import click

from rb_crawler.constant import State
from rb_crawler.rb_extractor import RbExtractor

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)


@click.command()
@click.option("-i", "--id", "rb_id", type=int, help="The rb_id to initialize the crawl from")
@click.option("-s", "--state", type=click.Choice(State), help="The state ISO code")
@click.option("--stop_id", "stop_rb_id", type=int, help="The rb_id to stop at")
def run(rb_id: int, state: State, stop_rb_id: int):
    if state == State.SCHLESWIG_HOLSTEIN:
        if rb_id < 7830:
            error = ValueError("The start rb_id for the state SCHLESWIG_HOLSTEIN (sh) is 7831")
            log.error(error)
            exit(1)
    RbExtractor(rb_id, state.value, stop_rb_id).extract()


if __name__ == "__main__":
    run()
