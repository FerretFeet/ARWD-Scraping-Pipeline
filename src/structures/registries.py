from collections.abc import Callable
from enum import Enum, auto
from typing import Any
from urllib.parse import urlparse


class PipelineRegistryKeys(Enum):
    """Enum defining pipeline registry keys, valid pages."""

    ARK_LEG_SEEDER = "arkleg.state.ar.us/"
    LEGISLATOR_LIST = "arkleg.state.ar.us/Legislators/List"
    LEGISLATOR = "arkleg.state.ar.us/Legislators/Detail"
    BILLS_SECTION = "arkleg.state.ar.us/Bills"
    BILL_CATEGORIES = "arkleg.state.ar.us/Bills/SearchByRange"
    BILL_LIST = "arkleg.state.ar.us/Bills/ViewBills"
    BILL = "arkleg.state.ar.us/Bills/Detail"
    BILL_VOTE = "arkleg.state.ar.us/Bills/Votes"

def get_enum_by_url(url: str) -> PipelineRegistryKeys:
    parsed_url = urlparse(url)
    cleaned_url = f"{parsed_url.netloc}{parsed_url.path}"

    # cleaned_url = cleaned_url.rstrip("/")

    # Sort the keys by length (longest value first) to ensure specific pages
    # (like 'Bills/Detail') match before generic ones (like 'Bills').
    sorted_keys = sorted(PipelineRegistryKeys, key=lambda k: len(k.value), reverse=True)

    for key in sorted_keys:
        # Check if the enum's value is contained within the cleaned input URL.
        # Example: 'https://arkleg.state.ar.us/Bills/Detail' is in 'https://arkleg.state.ar.us/Bills/Detail'
        # Example: 'https://arkleg.state.ar.us/Bills' is in 'https://arkleg.state.ar.us/Bills/ViewBills'
        # The key sorting prevents the second example from misclassifying.
        if key.value in cleaned_url:
            return key
    raise ValueError(f"URL '{url}' -> '{cleaned_url}' not found in PipelineRegistryKeys enum.")

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
