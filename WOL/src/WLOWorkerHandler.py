#!/usr/bin/env python
import datetime
import logging
import importlib
import importlib.util

ORCHESTRATION_PREFIX = 'OptimizerWorkLoad - '


def work(queue, worker_base_path):
    """
        work function to be triggered by the orchestration framework
        This work will consume value from internal queue repented by JSON
        The work message schema is -
        {
            "version":<the message version based on the orchestration version>
            "params":<key value object represented the params>
            "payload":<the actual work task>
        }
        The params section must contain workerDriver value to refer to (doing the actual work)
        The message will consumed by the WorkerDriver implemented under the Workers directory
    """

    logging.info(f"{ORCHESTRATION_PREFIX}Ready to work")

    while True:
        logging.debug(f"{ORCHESTRATION_PREFIX} Try to get some work task")

        ############################################ Metric ############################################################
        consume_work_time = datetime.datetime.now()
        ################################################################################################################
        try:
            work_msg = queue.get()
            driver_to_create = work_msg['params']["worker_driver"]

            try:
                spec = importlib.util.spec_from_file_location('init', f'{worker_base_path}/{driver_to_create}.py')
                clazz = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(clazz)
                worker_driver = clazz.init()
            except Exception as e:
                logging.error(f"{ORCHESTRATION_PREFIX}Could not find worker driver  driver : {driver_to_create}")
                raise Exception(e)

            worker_driver.work(work_msg)
            queue.task_done()


        except Exception as e:
            logging.error(f"{ORCHESTRATION_PREFIX}Error in Process - \n")
            logging.exception("message")
            queue.task_done()

    # todo - move to the sig exit part (gracefully end of life for the thread)
    logging.info(f"{ORCHESTRATION_PREFIX}Work Done")
