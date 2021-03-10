import argparse
from importlib import import_module
import multiprocessing
import json
import os
import logging


from OWL.OWLWorkerHandler import *






ORCHESTRATION_PREFIX = 'OptimizerWorkLoad - '


class ExecuterManager(object):
    """
    This is the Execute manager class - The orchestration manager that responsible to manage the work message creation
    and worker instantiation and triggering
    Class Properties -
          message_creator_driver_name - The driver that handel the work message creation - Should be implemented as need
          task_queue - JoinableQueue - sync queue for multi process works - will hold the works message that will
                       consumed by the orchestration workers
          process_list - List of instantiated process
          start_worker_first - Sign to the manager if we want to start the worker and then publish the JoinableQueue (fit to real time work)
                               Or to publish the queue first and then trigger the workers (fit to batch works)
          num_of_process - The num,ber of workers to trigger
    """

    # CTR
    def __init__(self, works_execution_params, message_creator_driver_name, num_of_threads, start_worker_first):

        self.message_creator_driver_name = message_creator_driver_name
        self.start_worker_first = start_worker_first
        self.task_queue = multiprocessing.JoinableQueue()
        self.process_list = []


        # In case of warm up we are trying to utilize much as we can the hosted machine
        # (more threads then cpu cores cause some of them will do some Network I/O work)
        if num_of_threads == -1:
            self.num_of_process = multiprocessing.cpu_count()
        else:
            self.num_of_process = num_of_threads
        logging.info(f"{ORCHESTRATION_PREFIX}Num of process to use - {self.num_of_process}")

        #Create instantiation of the message creator driver
        try:
            clazz = getattr(import_module(f'OWL.MessageCreatorWorkers.{message_creator_driver_name}'), 'MessageCreator')
            self.message_creator = clazz(works_execution_params)
        except KeyError as ke:
            logging.error(f"{ORCHESTRATION_PREFIX}Error on external params parsing - please validate that os enviornment variable - EXECUTION_PARAMS is a valid JSON format!")
            raise Exception(ke)
        except ModuleNotFoundError as mfe:
            logging.error(f"{ORCHESTRATION_PREFIX}Error during executor manager creation - can't find message creator driver - {self.message_creator_driver_name}")
            raise ModuleNotFoundError(mfe)
        except Exception as e:
            logging.error(f"{ORCHESTRATION_PREFIX}Error during executor manager creation")
            raise Exception(e)



    # The run function
    def run(self):
        """
        The run method -
            Step 1 - Start the workers (daemon processes) - if the flag is turned on
            Step 2 - Trigger the message creator driver to start filling the workers's queue
            Step 3 - Start the workers (daemon processes) - if the flag was turned off
            Step 4 - Join on the queue to wait till all the works will be consumed and Done
        :return:
        """
        logging.info(f"{ORCHESTRATION_PREFIX}Manager, Start To Run")
        #TODO - Move to parse boolean values instead of boolean str
        if self.start_worker_first == "true":
            logging.info(f"{ORCHESTRATION_PREFIX}Going to start the workers")
            self.start_workeres()
        logging.info(f"{ORCHESTRATION_PREFIX}Going to start insert some work to the queue")
        num_of_message_inserted = self.message_creator.create_messages(queue=self.task_queue)
        if num_of_message_inserted == 0:
            logging.warning(f"{ORCHESTRATION_PREFIX}No work messages were deliverd to the async queue - execution Done!!\n Bye Bye see you next time :)\n")
            return
        if self.start_worker_first == "false":
            logging.info(f"{ORCHESTRATION_PREFIX}Going to start the workers")
            self.start_workeres()
        # Block till all the process will Done
        #logging.info(f"{ORCHESTRATION_PREFIX}Going to wait till the queue will be empty")
        self.task_queue.join()
        logging.info("Queue is empty - worker were done")


    def start_workeres(self):
        """This method responsible to initiate the workers and trigger them
            Each wotker is a Deamon process that will trigger the work function from OWL.Worker
        """
        logging.info(f"{ORCHESTRATION_PREFIX}Starting workers ")
        # Create the threads and trigger them
        if len(self.process_list) == 0:
            for i in range(self.num_of_process):
                # Sent dummy var to the Process as args cause otherwise it's think that the queue should be
                # list or iterable Queue which is not!!!
                worker = multiprocessing.Process(target=work, args=(self.task_queue,'dummy'), name="Process-{}".format(i))
                # Mark the process as Daemon - once the Main process is done kill all the other
                worker.daemon = True
                worker.start()
                self.process_list.append(worker)





def log_setup():
    """This method setup the logging level an params
        logs output path can be controlled by the logstdout cmd param (stdout / file)
    """
    if args.logstdout == "false":
        logging.basicConfig(filename='BDP_Producing.log',
                            format='[%(asctime)s -%(levelname)s] (%(processName)-10s) %(message)s')
    else:
        logging.basicConfig(format='[%(asctime)s -%(levelname)s] (%(processName)-10s) %(message)s')

    log_level = args.logLevel

    if "LOG_LEVEL" in os.environ:
        log_level = os.environ['LOG_LEVEL']

    logging.getLogger().setLevel(log_level)
    logging.getLogger('boto3').setLevel(logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.ERROR)
    logging.getLogger('nose').setLevel(logging.ERROR)
    logging.getLogger("requests").setLevel(logging.ERROR)
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    logging.getLogger("s3fs").setLevel(logging.ERROR)
    logging.getLogger("s3transfer").setLevel(logging.ERROR)
    logging.getLogger("fsspec").setLevel(logging.ERROR)
    logging.getLogger('azure.storage').setLevel(logging.ERROR)

if __name__ == '__main__':

    #TODO - Work on desc for params
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--logLevel', required=False, type=str, default="INFO")
    parser.add_argument('--logstdout', required=False, type=str, default="false")
    parser.add_argument('--messageCreatorDriverName', required=True, type=str)
    parser.add_argument('--numOfThreads', required=False, type=int , default=-1)
    parser.add_argument('--startWorkersFirst', required=False, type=str, default='true')
    args = parser.parse_args()

    log_setup()

    try:
        works_execution_params = json.loads(os.environ["EXECUTION_PARAMS"])
        works_execution_params['numOfThreads'] = args.numOfThreads
        pm = ExecuterManager(works_execution_params, args.messageCreatorDriverName, args.numOfThreads, args.startWorkersFirst)
        pm.run()
        logging.info("Finished")
    except Exception as e:
        logging.exception(f"{ORCHESTRATION_PREFIX}Execution of ExecuterManager Fail!!!")
