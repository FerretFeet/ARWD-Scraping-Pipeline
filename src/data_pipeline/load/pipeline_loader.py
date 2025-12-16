"""Pipeline loader class."""

import datetime
import json
from pathlib import Path
from typing import Any, LiteralString

import psycopg
from psycopg import rows

from src.data_pipeline.transform.utils.strip_session_from_string import strip_session_from_link
from src.utils.logger import logger


class PipelineLoader:
    """Configuration object for a specific database loading operation."""

    def __init__(
        self,
        sql_file_path: Path,
        upsert_function_name: str,
        required_params: dict[str, type],
        insert: LiteralString,
        *,
        strict: bool = False,
    ) -> None:
        """Initialize PipelineLoader object."""
        self.sql_file_path: Path = sql_file_path
        self.upsert_function_name: str = upsert_function_name
        self.required_params: dict[str, type] = required_params
        self.insert = insert

        self.strict = strict

    def execute(self, params: dict, db_conn: psycopg.Connection) -> None | dict:
        """Prepare values for input and execute sql."""
        self.validate_input(params)
        prefixed_params = {f"p_{key}": value for key, value in params.items()}

        for k, val in prefixed_params.items():
            v = val
            if k == "p_url":
                v = strip_session_from_link(val, getSession=False)

            if isinstance(v, dict):
                prefixed_params[k] = json.dumps(v) if v else None
            elif isinstance(v, datetime.datetime):
                prefixed_params[k] = v.isoformat()
            elif isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
                prefixed_params[k] = [json.dumps(w) for w in v]
            else:
                prefixed_params[k] = v
        if isinstance(db_conn, psycopg.Connection):
            with db_conn.cursor(row_factory=rows.dict_row) as cur:
                cur.execute(self.insert, prefixed_params)
                db_conn.commit()
                return cur.fetchone()
        elif isinstance(db_conn, psycopg.Cursor):
            db_conn.execute(self.insert, prefixed_params)
            return db_conn.fetchone()
        return None

    def validate_input(self, input_params: dict[str, Any]) -> bool:
        """Ensure all required keys are present in the input dictionary."""
        missing_keys = [key for key in self.required_params if key not in input_params]
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
