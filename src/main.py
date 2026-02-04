"""Main.py: Main module."""

__author__ = 'B W'
from pathlib import Path

from src.bootstrap_sessions import insert_sessions, sessions_data, sql_function
from src.config.settings import PIPELINE_REGISTRY, known_links_cache_file, project_config
from src.data_pipeline.orchestrate import Orchestrator
from src.services.db_connect import db_conn
from src.structures.directed_graph import DirectionalGraph
from src.utils.logger import logger

STRICT = False
arklegbase = 'https://arkleg.state.ar.us/'
arklegsesquery = '?ddBienniumSession='
config = project_config


class Main:
    """Main class."""

    def __init__(self) -> None:
        """Create placeholders for state variables."""
        self.db_conn = db_conn
        self.state = DirectionalGraph()
        self.registry = PIPELINE_REGISTRY

        self.session_codes = None
        self.starting_links = None
        self.known_links_set = set()

    def setup(self) -> None:
        """Get starting values."""
        with self.db_conn() as conn:
            self.session_codes = insert_sessions(conn, sessions_data, sql_function)
        self.starting_links = [arklegbase + arklegsesquery + sc[0] for sc in self.session_codes]

    def load(self) -> None:
        """Load values from previous session(s)."""
        try:
            with Path.open(known_links_cache_file, 'r') as known_links_file:
                for line in known_links_file:
                    self.known_links_set.add(line.strip())
        except FileNotFoundError:
            logger.warning(
                f"Cache file '{known_links_cache_file}' not found. Starting with all links.",
            )

        self.starting_links = [link for link in self.starting_links if link not in self.known_links_set]

    def run(self):
        with db_conn() as conn:
            orchestrator = Orchestrator(
                self.registry,
                self.starting_links,
                self.conn,
                state=self.state,
            )
            orchestrator.orchestrate()

    def shutdown(self) -> None:
        """Shutdown threads and cleanup."""
        x = 1

    def main(self) -> None:
        """
        Run the scraper as preconfigured.

        Seed urls will be loaded and threads will be started.
        Runs until all urls are depleted.
        """
        self.setup()
        self.load()

        logger.info(f'starting_links: {self.starting_links}')

        self.run()

        self.shutdown()


if __name__ == '__main__':
    Main().main()
