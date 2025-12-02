"""
Config and path utilities for the German Credit DQ project.
"""

import os
import sys
from functools import lru_cache
from typing import Tuple, Dict, Any, Optional

import yaml

DEFAULT_CONFIG_FILE = "german_credit_config.yaml"


class ProjectConfig:
    """
    Wrapper around project path + YAML config.
    """

    def __init__(self, config_file: str = DEFAULT_CONFIG_FILE) -> None:
        self.config_file = config_file
        self.project_path = self._resolve_project_path()
        self._ensure_include_on_sys_path()
        self._config = self._load_config()

    # --------- internal helpers --------- #

    @staticmethod
    @lru_cache(maxsize=1)
    def _resolve_project_path() -> str:
        """Detect project root and cache it."""
        possible_paths = [
            "/opt/airflow",
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            os.getenv("PROJECT_PATH", "/opt/airflow"),
        ]

        project_path = None
        for path in possible_paths:
            include_dir = os.path.join(path, "include")
            if os.path.exists(path) and os.path.exists(include_dir):
                project_path = path
                break

        if project_path is None:
            project_path = os.getcwd()

        os.environ["PROJECT_PATH"] = project_path
        return project_path

    def _ensure_include_on_sys_path(self) -> None:
        """Make sure <project>/include is importable."""
        include_path = os.path.join(self.project_path, "include")
        if include_path not in sys.path:
            sys.path.insert(0, include_path)

    @property
    def dq_configs_dir(self) -> str:
        """Return path to include/dq_configs."""
        dq_dir = os.path.join(self.project_path, "include", "dq_configs")
        if not os.path.isdir(dq_dir):
            raise FileNotFoundError(f"`dq_configs` directory not found: {dq_dir}")
        return dq_dir

    def _load_config(self) -> Dict[str, Any]:
        """Load YAML config into a dict."""
        config_path = os.path.join(self.dq_configs_dir, self.config_file)
        if not os.path.exists(config_path):
            available = os.listdir(self.dq_configs_dir)
            raise FileNotFoundError(
                f"Config file not found: {config_path}\n"
                f"Available in dq_configs: {available}"
            )

        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

        return cfg or {}
    @property
    def env_path(self) -> str:
        """Return path to .env located in project root."""
        return os.path.join(self.project_path, ".env")

    @lru_cache(maxsize=1)
    def get_connection_string(self) -> str:
        """
        Get DB connection string.
        """
        return os.getenv('AIRFLOW_CONN_DQ_POSTGRES')
    
    @property
    def config(self) -> Dict[str, Any]:
        """Raw config dict."""
        return self._config

    # --------- path helpers --------- #

    @property
    def yaml_validation_path(self) -> str:
        """Return full path to YAML validation file."""
        yaml_file = self._config.get(
            "yaml_validation_file", "german_credit_validation.yaml"
        )
        yaml_path = os.path.join(self.dq_configs_dir, yaml_file)

        if not os.path.exists(yaml_path):
            available = os.listdir(self.dq_configs_dir)
            raise FileNotFoundError(
                f"YAML validation file not found: {yaml_path}\n"
                f"Available in dq_configs: {available}"
            )
        return yaml_path

    @property
    def csv_path(self) -> str:
        """Return full path to input CSV file."""
        csv_folder = self._config.get("csv_folder", "raw")
        csv_file = self._config.get("csv_file_for_dq", "german_credit_data.csv")

        csv_path = os.path.join(
            self.project_path, "include", "data", csv_folder, csv_file
        )
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        return csv_path

    def get_all_paths(self) -> Tuple[str, str]:
        """Return (yaml_validation_path, csv_path)."""
        return self.yaml_validation_path, self.csv_path

    # --------- config value helpers --------- #

    def get_corruption_function(self) -> Optional[str]:
        """Return corruption function name."""
        return self._config.get("corruption_function")

    def get_scenario(self) -> Optional[str]:
        """Return corruption scenario."""
        return self._config.get("corruption_scenario")

    def get_dq_threshold(self) -> Optional[float]:
        """Return DQ threshold as float if configured."""
        value = self._config.get("dq_threshold", 90.0)
        try:
            return float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"dq_threshold={value!r} is not numeric"
            ) from exc

    def get_run_id(self) -> Optional[str]:
        """Return run_id if configured."""
        return self._config.get("run_id")
    

    # --------- table name helpers --------- #

    @property
    def base_table_name(self) -> str:
        """Return base table name (database.table_name)."""
        return self._config.get("database", {}).get(
            "table_name", "german_credit_data"
        )

    @property
    def table_names(self) -> Dict[str, str]:
        """Return standardized names for all layers."""
        base = self.base_table_name
        return {
            "raw": f"raw_{base}",
            "corrupted": f"corrupted_{base}",
            "remediated": f"remediated_{base}",
            "metadata": f"{base}_metadata",
        }


# Simple singleton for the whole project 
@lru_cache(maxsize=1)
def get_default_config() -> ProjectConfig:
    """Return singleton ProjectConfig with default config file."""
    return ProjectConfig()
