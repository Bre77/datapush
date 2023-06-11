#!/usr/bin/env python

import sys
import os
import json
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), "..", 'lib'))
from splunklib.searchcommands import dispatch, StreamingCommand, Configuration, Option

@Configuration()
class datapushCommand(StreamingCommand):
    host=Option(
        doc='''
        **Syntax:** **host=***<hostname>*
        **Description:** The hostname (and port) to send data to.''',
        require=True
    )
    token=Option(
        doc='''
        **Syntax:** **token=***<string>*
        **Description:** The HEC token.''',
        require=True
    )

    def stream(self, events):
        data = [{
            "index": event["index"],
            "host": event["host"],
            "source": event["source"],
            "sourcetype": event["sourcetype"],
            "time": event["_time"],
            "event": event["_raw"]
        } for event in events]
        if data:
            r = requests.post(
                f"https://{self.host}/services/collector/event",
                data=json.dumps(data,separators=(",",":")),
                headers = {"Authorization": f"Splunk {self.token}"}
            )
            self.logger.info(f"response={r.status_code} code={r.json()['code']} count={len(data)}")
        yield from events

dispatch(datapushCommand, sys.argv, sys.stdin, sys.stdout, __name__)

