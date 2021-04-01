from abc import ABC, abstractmethod

class WorkerAbstract(ABC):
    """
        This class is the abstraction layer for the actual OWL workers
        Each application should implement worker that know how to work and consume the equivalent work message
    """

    @abstractmethod
    def work(self, msg):
        pass