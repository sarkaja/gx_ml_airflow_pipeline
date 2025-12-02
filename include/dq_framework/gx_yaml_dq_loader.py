# yaml_dq_loader.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Union
import yaml
import logging
import os
import great_expectations as gx
from great_expectations.core import ExpectationSuite

logger = logging.getLogger("airflow.task")


def snake_to_camel(s: str) -> str:
    """
    Convert snake_case string to CamelCase.
    
    Args:
        s (str): The snake_case string to convert.
        
    Returns:
        str: The converted CamelCase string.
    """
    return "".join(part.capitalize() for part in s.split("_"))


@dataclass
class DQYamlConfig:
    """
    Data Quality configuration container loaded from YAML.
    
    Attributes:
        data_asset_name (str): Name identifier for the data asset being validated.
        batch_definition_name (str): Name for the batch definition in Great Expectations.
        expectation_suite_name (str): Name of the expectation suite.
        expectations (List[Dict[str, Any]]): List of expectation configurations.
    """
    data_asset_name: str
    batch_definition_name: str
    expectation_suite_name: str
    expectations: List[Dict[str, Any]]


def gx_load_yaml_config(yaml_str_or_path: Union[str, os.PathLike]) -> DQYamlConfig:
    """
    Load Data Quality configuration from YAML string or file path.
    
    This function attempts to load YAML content either from a file path or directly
    from a YAML string. It provides sensible defaults for missing configuration values.
    
    Args:
        yaml_str_or_path (Union[str, os.PathLike]): File path or YAML string content.
        
    Returns:
        DQYamlConfig: Configuration object with loaded settings.

    """
    try:
        with open(yaml_str_or_path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
    except (FileNotFoundError, OSError, TypeError):
        # If file operations fail, treat as YAML string
        raw = yaml.safe_load(yaml_str_or_path)

    if not isinstance(raw, dict):
        raise ValueError("YAML content must parse into a dictionary/object structure.")

    data_asset_name = raw.get("data_asset_name", "default_data_asset")
    batch_definition_name = raw.get("batch_definition_name", "default_batch")
    suite_name = raw.get("expectation_suite_name", "default_validation_suite")
    expectations = raw.get("expectations", [])
     
    if not isinstance(expectations, list):
        raise ValueError("'expectations' must be a list of expectation definitions.")

    logger.info(f"Loaded configuration: {suite_name} with {len(expectations)} expectations")

    return DQYamlConfig(
        data_asset_name=data_asset_name,
        batch_definition_name=batch_definition_name,
        expectation_suite_name=suite_name,
        expectations=expectations,
    )


def create_expectation_objects(config: DQYamlConfig) -> List[Any]:
    """
    Create Great Expectations expectation objects from configuration.
    
    Args:
        config (DQYamlConfig): Configuration containing expectation definitions.
        
    Returns:
        List[Any]: List of instantiated Great Expectations expectation objects.
    """
    expectation_objects = []
    
    for item in config.expectations:
        if not isinstance(item, dict) or "expectation_type" not in item:
            raise ValueError("Each expectation must contain 'expectation_type' key")
            
        exp_type = item["expectation_type"]
        kwargs = item.get("kwargs", {})
        
        class_name = snake_to_camel(exp_type)
        
        try:
            ExpClass = getattr(gx.expectations, class_name)
            expectation_objects.append(ExpClass(**kwargs))
        except AttributeError:
            raise ValueError(f"Unknown expectation type: {exp_type} (class {class_name})")
        except TypeError as e:
            raise ValueError(f"Invalid arguments for {exp_type}: {e}")
    
    logger.info(f"Created {len(expectation_objects)} expectation objects")
    return expectation_objects