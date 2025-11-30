from collections.abc import Callable
from enum import Enum, auto


class PipelineRegistryKeys(Enum):
    """Enum defining pipeline registry keys."""

    ARK_LEG_SEEDER = "https://arkleg.state.ar.us/"
    LEGISLATOR_LIST = "https://arkleg.state.ar.us/Legislators/List"
    LEGISLATOR = "https://arkleg.state.ar.us/Legislators/Detail"
    BILLS_SECTION = "https://arkleg.state.ar.us/Bills"
    BILL_CATEGORIES = "https://arkleg.state.ar.us/Bills/SearchByRange"
    BILL_LIST = "https://arkleg.state.ar.us/Bills/ViewBills"
    BILL = "https://arkleg.state.ar.us/Bills/Detail"
    BILL_VOTE = "https://arkleg.state.ar.us/Bills/Votes"


class PipelineRegistries(Enum):
    """Enum defining pipeline registries."""

    FETCH = auto()
    PROCESS = auto()
    LOAD = auto()


PIPELINE_REGISTRY: dict[PipelineRegistryKeys, dict[PipelineRegistries, type | Callable]] = {}

for job_key in PipelineRegistryKeys:
    PIPELINE_REGISTRY[job_key] = {}


def register_processor(
    name: PipelineRegistryKeys,
    stage: PipelineRegistries,
) -> Callable[[type | Callable], type | Callable]:
    """
    Decorator to register a class or function into the two-level global registry.

    Args:
        name (PipelineRegistryKeys): The job type (e.g., BILL_VOTE).
        stage (PipelineRegistries): The pipeline stage (e.g., FETCH or PROCESS).

    """  # noqa: D401

    def decorator(cls_or_func: type | Callable) -> type | Callable:
        # 1. Check if the top-level job key exists (ensures initialization ran)
        if name not in PIPELINE_REGISTRY:
            msg = f"Job key '{name.name}' is not initialized in the global registry."
            raise ValueError(msg)
        name = name.value
        # 2. Check for duplicate registration at the specific stage
        if stage in PIPELINE_REGISTRY[name]:
            msg = f"Processor for job '{name.name}' and stage '{stage.name}' is already registered."
            raise ValueError(msg)

        # 3. Register the object at the two-level path
        PIPELINE_REGISTRY[name][stage] = cls_or_func

        return cls_or_func

    return decorator
