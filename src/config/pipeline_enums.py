from enum import Enum
from queue import LifoQueue, Queue


class PipelineRegistryKeys(Enum):
    """Enum defining pipeline registry keys, valid, expected pages."""

    ARK_LEG_SEEDER = "arkleg.state.ar.us/"
    LEGISLATOR_LIST = "arkleg.state.ar.us/Legislators/List"
    LEGISLATOR = "arkleg.state.ar.us/Legislators/Detail"
    BILLS_SECTION = "arkleg.state.ar.us/Bills"
    BILL_CATEGORIES = "arkleg.state.ar.us/Bills/SearchByRange"
    BILL_LIST = "arkleg.state.ar.us/Bills/ViewBills"
    BILL = "arkleg.state.ar.us/Bills/Detail"
    BILL_VOTE = "arkleg.state.ar.us/Bills/Votes"


class PipelineRegistries(Enum):
    """Enum defining pipeline stages and the queue type they use."""

    FETCH = ("FETCH", LifoQueue, "src.workers.pipeline_workers.CrawlerWorker")
    PROCESS = ("PROCESS", Queue, "src.workers.pipeline_workers.ProcessorWorker")
    LOAD = ("LOAD", Queue, "src.workers.pipeline_workers.LoaderWorker")


    def __init__(self, label, queue_type, worker_path):
        self.label = label
        self.queue_type = queue_type
        self.worker_path = worker_path

    def get_worker_class(self):
        import importlib
        module_name, cls_name = self.worker_path.rsplit(".", 1)
        module = importlib.import_module(module_name)
        return getattr(module, cls_name)
