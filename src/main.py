"""Main.py: Main module."""

__author__ = "B W"
from pathlib import Path

from src.bootstrap_sessions import insert_sessions, sessions_data, sql_function
from src.config.settings import PIPELINE_REGISTRY, known_links_cache_file, project_config
from src.data_pipeline.orchestrate import Orchestrator
from src.services.db_connect import db_conn
from src.structures.directed_graph import DirectionalGraph
from src.utils.logger import logger

STRICT = False
arklegbase = "https://arkleg.state.ar.us/"
arklegsesquery = "?ddBienniumSession="
config = project_config


def main() -> None:
    """
    Run the scraper as preconfigured.

    Seed urls will be loaded and threads will be started.
    Runs until all urls are depleted.
    """
    state = DirectionalGraph()
    with db_conn() as conn:
        session_codes = insert_sessions(conn, sessions_data, sql_function)
        registry = PIPELINE_REGISTRY
        starting_links = [arklegbase + arklegsesquery + sc[0] for sc in session_codes]
        known_links_set = set()
        try:
            with Path.open(known_links_cache_file, "r") as known_links_file:
                for line in known_links_file:
                    known_links_set.add(line.strip())
        except FileNotFoundError:
            logger.warning(
                f"Cache file '{known_links_cache_file}' not found. Starting with all links.",
            )

        # 2. Create a new list containing only the links not found in the set
        starting_links = [link for link in starting_links if link not in known_links_set]

        logger.info(f"starting_links: {starting_links}")

        orchestrator = Orchestrator(
            registry,
            starting_links,
            conn,
            state=state,
        )
        orchestrator.orchestrate()


if __name__ == "__main__":
    main()
