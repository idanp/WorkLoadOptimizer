from abc import ABC, abstractmethod

class MessageCreatorAbstract(ABC):
    """
        This is the Abstract class for OWL message creator
        Message form define as:
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

        params must contained the 2 pre defined keys , payload should be extend based on the implementation

        The Class have 3 parameters that the other sub class will inherit
            Worker_driver - which driver should consume the message that the class generate
            publish_metrics_flag - Flag to sign if we want to publish any metrics along the process
            metric_hanlder - the handler that will manage the entire metric publication in case of publish_metrics_flag=true


    """


    def __init__(self, params):
        self.publish_metrics_flag = params['publishMetricsFlag']
        self.worker_driver = params['workerDriver']
        self.metric_hanlder = None


    @abstractmethod
    def create_messages(self, queue):
        pass
