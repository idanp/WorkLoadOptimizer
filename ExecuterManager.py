import argparse
from importlib import import_module
import multiprocessing
import json
import os
import logging

from Utils.textUtils import *
from OWLWorkerHandler import *




VERSION='0.1'
ORCHESTRATION_PREFIX = 'OptimizerWorkLoad - '


class ExecuterManager(object):
    """
    This is the Execute manager class - The orchestration manager that responsible to manage the work message creation
    and worker instantiation and triggering
    Class Properties -
          message_creator_driver_full_path - Full path to the implementation of driver that handel the work message creation
          task_queue - JoinableQueue - sync queue for multi process works - will hold the works message that will
                       consumed by the orchestration workers
          process_list - List of instantiated process
          start_worker_first - Sign to the manager if we want to start the worker and then publish the JoinableQueue (fit to real time works)
                               Or to publish the queue first and then trigger the workers (fit to batch works)
          num_of_process - The num,ber of workers to trigger
          worker_base_path - The base path (on the file system ) where the worker drivers will located
    """

    # CTR
    def __init__(self, works_execution_params, worker_base_path, message_creator_driver_full_path, num_of_threads, start_worker_first):
        """
        Constructor
        :param works_execution_params: Optional - can get env variable for the specific execution and pass them to the msg creator phase
        :param worker_base_path:
        :param message_creator_driver_full_path:
        :param num_of_threads:
        :param start_worker_first:
        """

        self.message_creator_driver_name = message_creator_driver_full_path
        self.worker_base_path = worker_base_path
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
        # todo - get rid of the hard coded MessageCreator (instead get the name of the class as param)
        try:
            clazz = getattr(import_module(f'{message_creator_driver_full_path}'), 'MessageCreator')
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
            Step 1 - Start the workers (daemon processes) - if the start_workers_first flag is turned on
            Step 2 - Trigger the message creator driver to start filling the workers's queue
            Step 3 - Start the workers (daemon processes) - if the start_workers_first flag was turned off
            Step 4 - Join on the queue to wait till all the works will be consumed and Done
        :return:
        """
        logging.info(f"{ORCHESTRATION_PREFIX}Manager, Starting...")
        #TODO - Move to parse boolean values instead of boolean str
        if self.start_worker_first == "true":
            logging.info(f"{ORCHESTRATION_PREFIX}Going to start the workers")
            self.start_workeres()
        logging.info(f"{ORCHESTRATION_PREFIX}Going to start insert works to the queue")
        num_of_message_inserted = self.message_creator.create_messages(queue=self.task_queue)
        if num_of_message_inserted == 0:
            logging.warning(f"{ORCHESTRATION_PREFIX}No messages were deliverd to the async queue - execution Done!!\n Bye Bye see you next time :)\n")
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
                worker = multiprocessing.Process(target=work, args=(self.task_queue, self.worker_base_path), name="Process-{}".format(i))
                # Mark the process as Daemon - once the Main process is done kill all the other
                worker.daemon = True
                worker.start()
                self.process_list.append(worker)

def log_setup(list_of_ignored_logers=[]):
    """This method setup the logging level an params
        logs output path can be controlled by the log stdout cmd param (stdout / file)
    """
    if args.logstdout == "false":
        logging.basicConfig(filename='WLO.log',
                            format='[%(asctime)s -%(levelname)s] (%(processName)-10s) %(message)s')
    else:
        logging.basicConfig(format='[%(asctime)s -%(levelname)s] (%(processName)-10s) %(message)s')

    log_level = args.logLevel

    if "LOG_LEVEL" in os.environ:
        log_level = os.environ['LOG_LEVEL']

    logging.getLogger().setLevel(log_level)

    # Ignore loggers
    for log_name in list_of_ignored_logers:
        logging.getLogger(log_name).setLevel(logging.ERROR)

    logging.getLogger('boto3').setLevel(logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.ERROR)
    logging.getLogger('nose').setLevel(logging.ERROR)
    logging.getLogger("requests").setLevel(logging.ERROR)
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    logging.getLogger("s3fs").setLevel(logging.ERROR)
    logging.getLogger("s3transfer").setLevel(logging.ERROR)
    logging.getLogger("fsspec").setLevel(logging.ERROR)

def print_help():
    title = '''
======================================================================================================================================
=  ====  ====  ==================  =====  ===================  =========    ==========================================================
=  ====  ====  ==================  =====  ===================  ========  ==  =========================================================
=  ====  ====  ==================  =====  ===================  =======  ====  ==========  ============================================
=  ====  ====  ===   ===  =   ===  =  ==  ===   ====   ======  =======  ====  ==    ===    ==  ==  =  = ===  ==      ===   ===  =   ==
=   ==    ==  ===     ==    =  ==    ===  ==     ==  =  ===    =======  ====  ==  =  ===  =======        ==========  ==  =  ==    =  =
==  ==    ==  ===  =  ==  =======   ====  ==  =  =====  ==  =  =======  ====  ==  =  ===  ===  ==  =  =  ==  =====  ===     ==  ======
==  ==    ==  ===  =  ==  =======    ===  ==  =  ===    ==  =  =======  ====  ==    ====  ===  ==  =  =  ==  ====  ====  =====  ======
===    ==    ====  =  ==  =======  =  ==  ==  =  ==  =  ==  =  ========  ==  ===  ======  ===  ==  =  =  ==  ===  =====  =  ==  ======
====  ====  ======   ===  =======  =  ==  ===   ====    ===    =========    ====  ======   ==  ==  =  =  ==  ==      ===   ===  ======
======================================================================================================================================
'''

    text = (
        'Version- {} \n\n'
        'Welcome To Workload Optimizer\n'
        'Using this generic framework you can optimize and accelerate your workloads task executions\n'
        'Optimizer required two main interfaces:'
        '\t 1. MessgaeCreator - your python code that will implement the MessageCreatorAbstract\n'
        '\t 2. Worker - Your python code that will implement WorkerAbstract a work to react for a given message\n'
        'Once this execution managger will triggered with the given messageCreatorDriverPath and '
        '\n'.format(VERSION)
    )

    print(prep_title(title))
    print(
        '-------------------------------------------------------------------------------------------------------------------------------------------------------')
    print(prep_text(text))

if __name__ == '__main__':

    #TODO - Work on desc for params
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--logLevel', required=False, type=str, default="INFO")
    parser.add_argument('--loggerIgnore', required=False, type=str, default='')
    parser.add_argument('--logstdout', required=False, type=str, default="false")
    parser.add_argument('--messageCreatorDriverPath', required=True, type=str)
    parser.add_argument('--workersBasePath', required=True, type=str)
    parser.add_argument('--numOfThreads', required=False, type=int , default=-1)
    parser.add_argument('--startWorkersFirst', required=False, type=str, default='true')
    args = parser.parse_args()

    ignore_loggers = args.loggerIgnore.split(',')


    log_setup(list_of_ignored_logers=ignore_loggers)

    try:
        works_execution_params = {}
        if "EXECUTION_PARAMS" in os.environ:
            works_execution_params = json.loads(os.environ["EXECUTION_PARAMS"])
        works_execution_params['numOfThreads'] = args.numOfThreads
        pm = ExecuterManager(works_execution_params=works_execution_params,
                             message_creator_driver_full_path=args.messageCreatorDriverPath,
                             num_of_threads=args.numOfThreads,
                             start_worker_first=args.startWorkersFirst,
                             worker_base_path=args.workersBasePath)
        pm.run()
        logging.info("Finished")
    except Exception as e:
        logging.exception(f"{ORCHESTRATION_PREFIX}Execution of ExecuterManager Fail!!!")
