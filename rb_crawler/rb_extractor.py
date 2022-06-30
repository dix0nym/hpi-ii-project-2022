import logging
from time import sleep

import requests
from parsel import Selector

from build.gen.bakdata.corporate.v1.corporate_pb2 import Corporate, Status
from rb_producer import RbProducer

from datetime import date
import datetime 

log = logging.getLogger(__name__)

class RbExtractor:
    def __init__(self, start_rb_id: int, state: str, stop_rb_id :int):
        self.rb_id = start_rb_id
        self.stop_rb_id = stop_rb_id
        self.state = state
        self.producer = RbProducer()
        self.fail_count = 0
        self.end_date = date.today()
        self.today = False
        self.threshold = 2
        self.today_threshold = 2
	
    def extract(self):
        while True:
            if self.rb_id > self.stop_rb_id: 
                break
            try:
                log.info(f"Sending Request for: {self.rb_id} and state: {self.state}")
                text = self.send_request()
                if "Falsche Parameter" in text:
                    self.fail_count += 1
                    log.info(f"Falsche Parameter count: {self.fail_count}")
                    if self.today: 
                        if self.fail_count > self.today_threshold: 
                            break
                    else: 
                        if self.fail_count > self.threshold: 
                            break
                    self.rb_id += 1
                    continue
                self.fail_count = 0
                selector = Selector(text=text)
                corporate = Corporate()
                corporate.rb_id = self.rb_id
                corporate.state = self.state
                corporate.reference_id = self.extract_company_reference_number(selector)
                event_type = selector.xpath("/html/body/font/table/tr[3]/td/text()").get()
                corporate.event_date = selector.xpath("/html/body/font/table/tr[4]/td/text()").get()
                edate = datetime.datetime.strptime(corporate.event_date, "%d.%m.%Y").date()
                if edate >= self.end_date: 
                    self.today = True 
                corporate.id = f"{self.state}_{self.rb_id}"
                raw_text: str = selector.xpath("/html/body/font/table/tr[6]/td/text()").get()
                raw_explanation: str = selector.xpath("/html/body/font/table/tr[5]/td/text()").get()
                if raw_explanation:
                    raw_text = raw_explanation + ":info:" + raw_text
                self.handle_events(corporate, event_type, raw_text)
                self.rb_id = self.rb_id + 1
                log.debug(corporate)
            except Exception as ex:
                log.error(f"Skipping {self.rb_id} in state {self.state}")
                log.error(f"Cause: {ex}")
                self.rb_id = self.rb_id + 1
                continue
        exit(0)

    def send_request(self) -> str:
        url = f"https://www.handelsregisterbekanntmachungen.de/skripte/hrb.php?rb_id={self.rb_id}&land_abk={self.state}"
        # For graceful crawling! Remove this at your own risk!
        sleep(0.1)
        return requests.get(url=url).text

    @staticmethod
    def extract_company_reference_number(selector: Selector) -> str:
        return ((selector.xpath("/html/body/font/table/tr[1]/td/nobr/u/text()").get()).split(": ")[1]).strip()

    def handle_events(self, corporate, event_type, raw_text):
        corporate.information = raw_text
        if event_type == "Neueintragungen":
            self.handle_new_entries(corporate, raw_text)
        elif event_type == "Veränderungen":
            self.handle_changes(corporate, raw_text)
        elif event_type == "Löschungen":
            self.handle_deletes(corporate)
        else:
            self.handle_unknown(corporate, raw_text, event_type)

    def handle_unknown(self, corporate: Corporate, raw_text: str, event_type: str) -> Corporate: 
        log.debug(f"unknown entry")
        corporate.information = raw_text 
        corporate.event_type = event_type
        self.producer.produce_to_topic(corporate=corporate)

    def handle_new_entries(self, corporate: Corporate, raw_text: str) -> Corporate:
        log.debug(f"New company found: {corporate.id}")
        corporate.event_type = "create"
        corporate.information = raw_text
        corporate.status = Status.STATUS_ACTIVE
        self.producer.produce_to_topic(corporate=corporate)

    def handle_changes(self, corporate: Corporate, raw_text: str):
        log.debug(f"Changes are made to company: {corporate.id}")
        corporate.event_type = "update"
        corporate.status = Status.STATUS_ACTIVE
        corporate.information = raw_text
        self.producer.produce_to_topic(corporate=corporate)

    def handle_deletes(self, corporate: Corporate):
        log.debug(f"Company {corporate.id} is inactive")
        corporate.event_type = "delete"
        corporate.status = Status.STATUS_INACTIVE
        self.producer.produce_to_topic(corporate=corporate)
