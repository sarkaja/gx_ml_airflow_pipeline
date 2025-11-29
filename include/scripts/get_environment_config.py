import sys
import os
import yaml
from typing import Tuple, Dict

def get_project_path():
    """Environment setup for the whole project"""
    
    possible_paths = [
        '/opt/airflow', 
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
        os.getenv('PROJECT_PATH', '/opt/airflow'),  
    ]
    
    project_path = None
    for path in possible_paths:
        if os.path.exists(path) and os.path.exists(os.path.join(path, 'include')):
            project_path = path
            break
    
    if project_path is None:
        project_path = os.getcwd()

    include_path = os.path.join(project_path, 'include')
    if include_path not in sys.path:
        sys.path.insert(0, include_path)
    
    os.environ['PROJECT_PATH'] = project_path
    
    return project_path


def load_config(config_file: str = "german_credit_config.yaml") -> Dict:
    """
    Load configuration from YAML file.
    
    Args:
        config_file: Name of config YAML file (default: "german_credit_config.yaml")
        
    Returns:
        Dictionary with configuration values
    """
    project_path = get_project_path()
    config_path = os.path.join(project_path, 'include', 'dq_configs', config_file)
    
    # Verify config file exists
    if not os.path.exists(config_path):
        available_files = os.listdir(os.path.join(project_path, 'include', 'dq_configs'))
        raise FileNotFoundError(
            f"Config file not found: {config_path}\n"
            f"Available files in dq_configs: {available_files}"
        )
    
    # Load YAML config
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    return config



def get_yaml_path_from_config(config_file: str = "german_credit_config.yaml") -> str:
    """
    Returns path to YAML validation file from config.
    
    Args:
        config_file: Name of config YAML file
        
    Returns:
        Full path to YAML validation file
    """
    config = load_config(config_file)
    yaml_validation_file = config.get('yaml_validation_file', 'german_credit_validation.yaml')
    
    project_path = get_project_path()
    yaml_path = os.path.join(project_path, 'include', 'dq_configs', yaml_validation_file)
    
    # Verify YAML file exists
    if not os.path.exists(yaml_path):
        available_files = os.listdir(os.path.join(project_path, 'include', 'dq_configs'))
        raise FileNotFoundError(
            f"YAML validation file not found: {yaml_path}\n"
            f"Available files in dq_configs: {available_files}"
        )
    
    return yaml_path


def get_csv_path_from_config(config_file: str = "german_credit_config.yaml") -> str:
    """
    Returns path to CSV file from config.
    
    Args:
        config_file: Name of config YAML file
        
    Returns:
        Full path to CSV file
    """
    config = load_config(config_file)
    csv_folder = config.get('csv_folder', 'raw')
    csv_file_for_dq = config.get('csv_file_for_dq', 'german_credit_data.csv')
    
    project_path = get_project_path()
    csv_path = os.path.join(project_path, 'include', 'data', csv_folder, csv_file_for_dq)
    
    # Verify CSV file exists
    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"CSV file not found: {csv_path}\n"
        )
    
    return csv_path

def get_all_paths_from_config(config_file: str = "german_credit_config.yaml") -> Tuple[str, str]:
    """
    Returns both paths at once - YAML and CSV from config.
    
    Args:
        config_file: Name of config YAML file
        
    Returns:
        Tuple (yaml_path, csv_path)
    """
    yaml_path = get_yaml_path_from_config(config_file)
    csv_path = get_csv_path_from_config(config_file)
    
    return yaml_path, csv_path


def get_config_values(config_file: str = "german_credit_config.yaml") -> Dict:
    """
    Returns all configuration values.
    
    Args:
        config_file: Name of config YAML file
        
    Returns:
        Dictionary with all config values
    """
    config = load_config(config_file)
    return {
        'csv_folder': config.get('csv_folder', 'raw'),
        'csv_file_for_dq': config.get('csv_file_for_dq', 'german_credit_data.csv'),
        'yaml_validation_file': config.get('yaml_validation_file', 'german_credit_validation.yaml')
    }