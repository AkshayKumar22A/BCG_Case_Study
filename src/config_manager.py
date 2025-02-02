import yaml
import os
from typing import Dict, Any

class ConfigManager:
    """Manages loading and validation of YAML configuration."""
    
    def __init__(self, config_path: str = "config/config.yml"):
        """
        Args:
            config_path: Path to YAML configuration file
        Raises:
            FileNotFoundError: If config file is missing
            KeyError: If required sections are missing
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load and validate configuration structure."""
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Ensure required configuration sections exist
        required_keys = ['data', 'analyses']
        for key in required_keys:
            if key not in config:
                raise KeyError(f"Missing required config section: {key}")
                
        return config

    def get_data_config(self) -> Dict[str, str]:
        """Get data-related configuration.
        - input_path: Base directory for input files
        - output_path: Directory for analysis results
        - Dataset-specific filenames (primary_person.csv, etc.)
        """
        return self.config['data']

    def get_analyses_config(self) -> list:
        """Get list of analyses to execute.
        Returns ordered list of analysis method names to execute.
        Example: ['analysis_1', 'analysis_2', ...]
        """
        return self.config['analyses']

    def get_log_level(self) -> str:
        """Returns logging level (default: INFO)."""
        return self.config.get('log_level', 'INFO').upper()