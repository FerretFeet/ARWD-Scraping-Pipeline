"""Class to create a registry. Central registry for all pipeline stages."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from src.config.pipeline_enums import PipelineRegistries, PipelineRegistryKeys
from src.data_pipeline.load.pipeline_loader import PipelineLoader

if TYPE_CHECKING:
    from queue import Queue


def get_enum_by_url(url: str) -> PipelineRegistryKeys:
    """Pass in a url and get the matching PipelineRegistryKeys Enum."""
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
    msg = f"URL '{url}' -> '{cleaned_url}' not found in PipelineRegistryKeys enum."
    raise ValueError(msg)


# Type Alias
type ProcessorType = type[Any] | Callable[..., Any] | dict


class ProcessorRegistry:
    """A centralized registry to manage pipeline processors."""

    def __init__(self) -> None:
        """Initialize a processor registry."""
        self._registry: dict[PipelineRegistryKeys, dict[PipelineRegistries, ProcessorType]] = {
            key: {} for key in PipelineRegistryKeys
        }

    def register(
        self,
        name: PipelineRegistryKeys,
        stage: PipelineRegistries,
        **attrs: dict,
    ) -> Any:
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
            self._registry[name][stage] = {
                "processor": cls_or_func,
                "attrs": attrs,
            }
            return cls_or_func

        return decorator

    def get_processor(
        self,
        name: PipelineRegistryKeys,
        stage: PipelineRegistries,
    ) -> ProcessorType | PipelineLoader | None:
        """Retrieve a processor. Raises error if not found."""
        try:
            processor = self._registry[name][stage]["processor"]
            if isinstance(processor, type):
                return processor()
        except KeyError as err:
            msg = f"No processor found for {name.name} at stage {stage.name}"
            raise KeyError(msg) from err

    def get_all(self) -> dict:
        """Read-only view of the registry (optional utility)."""
        return self._registry

    def get_attrs(self, name: PipelineRegistryKeys, stage: PipelineRegistries) -> dict[str, Any]:
        """Retrieve a registry's attributes. Raises error if not found."""
        try:
            return self._registry[name][stage]["attrs"]
        except KeyError as err:
            msg = f"No attributes found for {name.name} at stage {stage.name}"
            raise ValueError(msg) from err

    def get_queue_type(self, stage: PipelineRegistries) -> type[Queue]:
        """Return the queue type associated with this pipeline stage."""
        return stage.queue_type

    def load_p_config(self, config: dict) -> None:
        """
        Load a config mapping:
            {PipelineRegistryKey: {Stage: Processor}}.
        """  # noqa: D205
        for key, stage_map in config.items():
            for stage, processor in stage_map.items():
                self.register(key, stage)(processor)

    def load_l_config(self, config: dict) -> None:
        """
        Load a config mapping:.

            {PipelineRegistryKey: {Stage: Processor}}
        """

        def create_loader_class(key: PipelineRegistryKeys, stage_map: dict) -> None:
            class Loader(PipelineLoader):
                def __init__(self) -> None:
                    super().__init__(
                        stage_map["filepath"],
                        stage_map["name"],
                        stage_map["params"],
                        stage_map["insert"],
                    )

            self.register(key, PipelineRegistries.LOAD)(Loader)

        # --- End of Factory Function ---

        for key, stage_map in config.items():
            create_loader_class(key, stage_map)

        if not config:
            msg = f"No config found for {PipelineRegistries.LOAD}"
            raise ValueError(msg)
