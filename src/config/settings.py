"""Settings module."""

from src.config.registry_config import PROCESSOR_CONFIG, LOADER_CONFIG
from src.structures.registries import ProcessorRegistry
from src.utils.paths import project_root

# ------ GLOBAL VARS -------
PIPELINE_STRICT = True
cache_dir = project_root / "cache"
state_cache_file = cache_dir / "state_cache.json"
known_links_cache_file = cache_dir / "known_links_cache.json"
seed_links = ["https://arkleg.state.ar.us"]
config = {
    "strict": PIPELINE_STRICT,
    "state_cache_file": state_cache_file,
    "known_links_cache_file": known_links_cache_file,
}

PIPELINE_REGISTRY = ProcessorRegistry()
PIPELINE_REGISTRY.load_p_config(PROCESSOR_CONFIG)
PIPELINE_REGISTRY.load_l_config(LOADER_CONFIG)



