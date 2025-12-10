import json
from pathlib import Path
from typing import Any, LiteralString

import psycopg

from src.utils.logger import logger


class PipelineLoader:
    """
    Configuration object for a specific database loading operation.
    """

    def __init__(self, sql_file_path: Path, upsert_function_name: str,
                 required_params: dict[str, type], insert: LiteralString, *, strict: bool = False):
        self.sql_file_path: Path = sql_file_path
        self.upsert_function_name: str = upsert_function_name
        self.required_params: dict[str, type] = required_params
        self.insert = insert

        self.strict = strict
        self._template = None

    def execute(self, params: dict, db_conn: psycopg.Connection) -> None | dict:
        self.validate_input(params)
        prefixed_params = {f"p_{key}": value for key, value in params.items()}
        for k, v in prefixed_params.items():
            if isinstance(v, dict):
                prefixed_params[k] = json.dumps(v)
        sql = self.sql_template
        if isinstance(db_conn, psycopg.Connection):
            with db_conn.cursor() as cur:
                cur.execute(self.insert, prefixed_params)
                db_conn.commit()
                return cur.fetchone()
        elif isinstance(db_conn, psycopg.Cursor):
            db_conn.execute(self.insert, prefixed_params)
            return db_conn.fetchone()
        return None



    def validate_input(self, input_params: dict[str, Any]) -> bool:
        """Ensure all required keys are present in the input dictionary."""
        missing_keys = [
            key for key in self.required_params
            if key not in input_params
        ]
        if missing_keys:
            msg = (
                f"Loader for {self.upsert_function_name} is missing required parameters: "
                f"{', '.join(missing_keys)}"
            )
            logger.warning(msg)
            if self.strict:
                raise ValueError(
                    msg,
                )
            return False
        return True

    @property
    def sql_template(self) -> str:
        if not self._template:
            self._template = self.sql_file_path.read_text()
        return self._template
