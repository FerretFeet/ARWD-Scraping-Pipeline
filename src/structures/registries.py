from collections.abc import Callable
from enum import Enum, auto
from typing import Any


class PipelineRegistryKeys(Enum):
    """Enum defining pipeline registry keys, valid pages."""

    ARK_LEG_SEEDER = "https://arkleg.state.ar.us/"
    LEGISLATOR_LIST = "https://arkleg.state.ar.us/Legislators/List"
    LEGISLATOR = "https://arkleg.state.ar.us/Legislators/Detail"
    BILLS_SECTION = "https://arkleg.state.ar.us/Bills"
    BILL_CATEGORIES = "https://arkleg.state.ar.us/Bills/SearchByRange"
    BILL_LIST = "https://arkleg.state.ar.us/Bills/ViewBills"
    BILL = "https://arkleg.state.ar.us/Bills/Detail"
    BILL_VOTE = "https://arkleg.state.ar.us/Bills/Votes"

def get_enum_by_url(url: str) -> PipelineRegistryKeys:
    # Iterate over the enum members and return the matching one
    for key in PipelineRegistryKeys:
        if key.value == url:
            return key
    raise ValueError(f"URL '{url}' not found in PipelineRegistryKeys enum.")

class PipelineRegistries(Enum):
    """Enum defining pipeline registries. Pipeline steps."""

    FETCH = auto()
    PROCESS = auto()
    LOAD = auto()
# Type Alias
type ProcessorType = type[Any] | Callable[..., Any]

class ProcessorRegistry:
    """A centralized registry to manage pipeline processors."""

    def __init__(self) -> None:
        # Initialize the internal storage
        self._registry: dict[PipelineRegistryKeys, dict[PipelineRegistries, ProcessorType]] = {
            key: {} for key in PipelineRegistryKeys
        }

    def register(self, name: PipelineRegistryKeys, stage: PipelineRegistries):
        """
        Decorator method to register a function or class.

        Usage: @registry.register(Key, Stage)
        """  # noqa: D401
        def decorator(cls_or_func: ProcessorType) -> ProcessorType:
            # 1. Validation: Check duplicates
            if stage in self._registry[name]:
                existing = self._registry[name][stage]
                msg = (
                    f"Processor for '{name.name}' and stage '{stage.name}' "
                    f"is already registered to: {existing}"
                )
                raise ValueError(msg)

            # 2. Registration
            self._registry[name][stage] = cls_or_func
            return cls_or_func

        return decorator

    def get_processor(self, name: PipelineRegistryKeys, stage: PipelineRegistries) -> ProcessorType:
        """Retrieve a processor. Raises error if not found."""
        try:
            return self._registry[name][stage]
        except KeyError as err:
            msg = f"No processor found for {name.name} at stage {stage.name}"
            raise ValueError(msg) from err

    def get_all(self) -> dict:
        """Read-only view of the registry (optional utility)."""
        return self._registry
