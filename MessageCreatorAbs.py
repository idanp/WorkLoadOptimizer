from abc import ABC, abstractmethod

class MessageCreatorAbstract(ABC):
    """
        This is the Abstract class for WLO message creator
        Message structure defined as:
        {
            params:
                    {
                    publishMetricsFlag:boolean as string
                    worker_driver: string
                    },
            payload:
                    {

                    }

        }

        params must contained the 2 pre defined keys: (can configure them as OS env var )
            1. publishMetricsFlag - does the system should publish metrics to other system
            2. worker_driver - which driver to use for the coming generated messages
        payload should be extend based on the implementation - The relevant payload that the worker know to parse and
        work accordingly

        The Class have 3 properties that the sub class will inherit
            Worker_driver - which driver should consume the message that the class generate
            publish_metrics_flag - Flag to sign if we want to publish any metrics along the process
            metric_hanlder - the handler that will manage the entire metric publication in case of publish_metrics_flag=true


        Inherit class must be called MessageCreator



    """


    def __init__(self, params):
        if 'publishMetricsFlag' in params:
            self.publish_metrics_flag = params['publishMetricsFlag']
        else:
            self.publish_metrics_flag = None
        if 'workerDriver' in params:
            self.worker_driver = params['workerDriver']
        else:
            self.worker_driver = None
        self.metric_hanlder = None


    @abstractmethod
    def create_messages(self, queue):
        pass
