import sys
import os

def setup_environment():
    """Environment for the whole project"""
    
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