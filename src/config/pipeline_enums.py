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

    FETCH = ("FETCH", LifoQueue)
    PROCESS = ("PROCESS", Queue)
    LOAD = ("LOAD", Queue)

    def __init__(self, label: str, queue_type: type[Queue]):
        self.queue_type = queue_type
