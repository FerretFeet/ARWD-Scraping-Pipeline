"""Settings module."""

from src.utils.paths import project_root

PIPELINE_STRICT = True
cache_dir = project_root / "cache"
state_cache_file = cache_dir / "state_cache.json"
known_links_cache_file = cache_dir / "known_links_cache.json"

config = {
    "strict": PIPELINE_STRICT,
    "state_cache_file": state_cache_file,
    "known_links_cache_file": known_links_cache_file,
}
