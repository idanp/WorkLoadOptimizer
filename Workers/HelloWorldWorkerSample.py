from WorkerAbs import *
import logging

WORKER_DRIVER_PREFIX = "Worker-Driver-HelloWorld:  "
class WorkerImpl(WorkerAbstract):

    def work(self, msg):
        working_params = msg["params"]

        payload = msg['payload']
        capital_yes_no = working_params['capital_letter']
        txt = payload['text']

        if capital_yes_no:
            logging.info(txt.upper())
        else:
            logging.info(txt)