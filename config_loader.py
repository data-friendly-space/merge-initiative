import json
import os
import sys
from pathlib import Path

def find_project_root(current_path, marker_file='config.sample.json'):
    """Traverse up the directory tree to find the project root."""
    current_path = Path(current_path).resolve()
    while True:
        if (current_path / marker_file).exists():
            return current_path
        parent = current_path.parent
        if parent == current_path:
            return None
        current_path = parent

def get_project_root():
    # First, check if PROJECT_ROOT environment variable is set
    env_root = os.environ.get('PROJECT_ROOT')
    if env_root:
        return Path(env_root).resolve()
    
    # If not, try to find the project root from the current file's location
    file_path = Path(__file__).resolve()
    found_root = find_project_root(file_path)
    if found_root:
        return found_root
    
    # If still not found, try from the main script's location
    main_script_path = Path(sys.argv[0]).resolve()
    found_root = find_project_root(main_script_path)
    if found_root:
        return found_root
    
    raise FileNotFoundError("Could not determine project root. Please set PROJECT_ROOT environment variable.")

def load_config():
    # Find the project root directory
    root_dir = find_project_root(Path(__file__).parent)

    # Try to load the config file
    config_path = root_dir / 'config.json'
    if config_path.exists():
        with open(config_path, 'r') as f:
            config = json.load(f)
    else:
        # If config.json doesn't exist, load the sample config
        sample_config_path = root_dir / 'config.sample.json'
        with open(sample_config_path, 'r') as f:
            config = json.load(f)

    # Override with environment variables
    for key in config:
        env_var = os.environ.get(key.upper())
        if env_var:
            config[key] = env_var

    return config

# Global config object
CONFIG = load_config()