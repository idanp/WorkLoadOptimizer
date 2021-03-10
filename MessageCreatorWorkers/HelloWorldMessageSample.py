from MessageCreatorAbs import *
#from Utils.MetricHandler import *

class MessageCreator(MessageCreatorAbstract):

    def __init__(self, params):
        super().__init__(params)

        self.capital_letter = params['capitalYesNo']
        self.number_of_messages = params['totalNumOfMsg']



    def create_messages(self, queue):

        for i in range(0,self.number_of_messages):
            msg = dict(
                params=dict(
                    publish_metrics_flag=self.publish_metrics_flag if self.publish_metrics_flag else "false",
                    worker_driver=self.worker_driver if self.worker_driver else "HelloWorldWorkerSample",
                    capital_letter=self.capital_letter

                ),
                payload=dict(
                    text=f"Hello World - Bob {i}"

                )

            )

            queue.put(msg)



