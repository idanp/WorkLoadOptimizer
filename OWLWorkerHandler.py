import datetime
import logging
import importlib




ORCHESTRATION_PREFIX = 'OptimizerWorkLoad - '



def work(queue,bla):
    """
        work method to be triggered by the orchestration framework
        This work will consume value from internal queue repented by JSON
        The work message schema is -
        {
            "version":<the message version based on the oracstracion version>
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
        generation_for_tenant_for_specific_timeframe_start_time = datetime.datetime.now()
        ################################################################################################################
        try:
            work_msg = queue.get()
            driver_to_create = work_msg['params']["worker_driver"]

            try:
                clazz = getattr(importlib.import_module(f'OWL.Workers.{driver_to_create}', package=None),
                                'WorkerImpl')
                worker_driver  = clazz()
            except Exception as e:
                logging.error(f"{ORCHESTRATION_PREFIX}Could not find worker driver  driver : {driver_to_create}")
                raise Exception(e)

            worker_driver.work(work_msg)
            queue.task_done()


        except Exception as e:
            logging.error(f"{ORCHESTRATION_PREFIX}Error in Process - \n")
            logging.exception("message")
            queue.task_done()


    logging.info(f"{ORCHESTRATION_PREFIX}Work Done")

