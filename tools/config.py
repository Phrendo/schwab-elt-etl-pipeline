"""
Configuration Management Module

This module provides centralized configuration management for the Schwab API project.
It combines secrets from .env files with structured configuration from config.yaml,
providing a unified interface for accessing all application settings.

Features:
- Loads secrets from .env files (credentials, account numbers, etc.)
- Loads structured configuration from config.yaml
- Provides type-safe access to configuration values
- Supports default values and validation
- Caches configuration for performance

Dependencies:
- PyYAML: For parsing YAML configuration files
- python-dotenv: For loading environment variables from .env files
- os: For environment variable access

Usage:
    from tools.config import Config
    
    config = Config()
    
    # Access secrets from .env
    username = config.get_secret('SQL_USERNAME')
    
    # Access configuration from config.yaml
    api_name = config.get('application.api.data_name')
    timeout = config.get('schwab_api.http_timeout', default=10)
    
    # Access service-specific configuration
    balance_config = config.get_service_config('balance_service')
"""

import os
import yaml
from typing import Any, Dict, Optional, Union
from dotenv import load_dotenv, find_dotenv


class Config:
    """
    Centralized configuration manager for the Schwab API project.
    
    Combines secrets from .env files with structured configuration from config.yaml,
    providing a unified interface for accessing all application settings.
    """
    
    def __init__(self, config_file: str = "config.yaml"):
        """
        Initialize the configuration manager.
        
        Args:
            config_file: Path to the YAML configuration file (default: "config.yaml")
            
        Raises:
            FileNotFoundError: If config.yaml file is not found
            yaml.YAMLError: If config.yaml file is malformed
        """
        # Load environment variables from .env file
        load_dotenv(find_dotenv(), override=True)
        
        # Load YAML configuration
        self.config_file = config_file
        self._yaml_config = self._load_yaml_config()
        
        # Cache for performance
        self._cache = {}
    
    def _load_yaml_config(self) -> Dict[str, Any]:
        """
        Load and parse the YAML configuration file.
        
        Returns:
            Dictionary containing the parsed YAML configuration
            
        Raises:
            FileNotFoundError: If config.yaml file is not found
            yaml.YAMLError: If config.yaml file is malformed
        """
        try:
            with open(self.config_file, 'r', encoding='utf-8') as file:
                return yaml.safe_load(file) or {}
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file '{self.config_file}' not found")
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error parsing configuration file '{self.config_file}': {e}")
    
    def get_secret(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get a secret value from environment variables.
        
        Secrets include credentials, account numbers, and other sensitive data
        that should not be stored in configuration files.
        
        Args:
            key: Environment variable name
            default: Default value if environment variable is not set
            
        Returns:
            The secret value or default if not found
        """
        return os.getenv(key, default)
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value using dot notation.
        
        Args:
            key: Configuration key using dot notation (e.g., 'schwab_api.max_retries')
            default: Default value if key is not found
            
        Returns:
            The configuration value or default if not found
            
        Examples:
            config.get('application.name')
            config.get('schwab_api.max_retries', 30)
            config.get('balance_service.check_times')
        """
        # Check cache first
        if key in self._cache:
            return self._cache[key]
        
        # Navigate through nested dictionary using dot notation
        keys = key.split('.')
        value = self._yaml_config
        
        try:
            for k in keys:
                value = value[k]
            
            # Cache the result
            self._cache[key] = value
            return value
            
        except (KeyError, TypeError):
            return default
    
    def get_service_config(self, service_name: str) -> Dict[str, Any]:
        """
        Get the complete configuration for a specific service.
        
        Args:
            service_name: Name of the service (e.g., 'balance_service', 'ohlc_service')
            
        Returns:
            Dictionary containing all configuration for the service
            
        Examples:
            balance_config = config.get_service_config('balance_service')
            check_times = balance_config.get('check_times', [])
        """
        return self.get(service_name, {})
    
    def get_database_config(self) -> Dict[str, Any]:
        """
        Get complete database configuration combining secrets and settings.
        
        Returns:
            Dictionary containing database connection configuration
        """
        # Get secrets from environment
        username = self.get_secret('SQL_USERNAME')
        password = self.get_secret('SQL_PASSWORD')
        host = self.get_secret('SQL_HOST')
        
        if not username or not password:
            missing = []
            if not username:
                missing.append('SQL_USERNAME')
            if not password:
                missing.append('SQL_PASSWORD')
            raise ValueError(f"Missing required database credentials: {', '.join(missing)}")
        
        # Get configuration from YAML
        db_config = self.get_service_config('database')
        
        # Combine secrets and configuration
        return {
            'username': username,
            'password': password,
            'host': host,
            'port': db_config.get('port', 1433),
            'database': db_config.get('database_name', 'OPT'),
            'driver': db_config.get('driver', 'ODBC Driver 17 for SQL Server')
        }
    
    def get_email_config(self) -> Dict[str, Any]:
        """
        Get complete email configuration combining secrets and settings.
        
        Returns:
            Dictionary containing email configuration
        """
        # Get secrets from environment
        email_username = self.get_secret('EMAIL_USERNAME')
        email_password = self.get_secret('EMAIL_PASSWORD')
        notification_email = self.get_secret('NOTIFICATION_EMAIL')
        
        # Get configuration from YAML
        email_config = self.get_service_config('email')
        
        return {
            'username': email_username,
            'password': email_password,
            'notification_email': notification_email,
            'enabled': email_config.get('enabled', True)
        }
    
    def get_api_config(self) -> Dict[str, Any]:
        """
        Get API configuration including names and settings.
        
        Returns:
            Dictionary containing API configuration
        """
        app_config = self.get_service_config('application')
        api_config = app_config.get('api', {})
        schwab_config = self.get_service_config('schwab_api')
        
        return {
            'data_name': api_config.get('data_name', 'MAIN_DATA'),
            'trade_name': api_config.get('trade_name', 'MAIN_TRADE'),
            'max_retries': schwab_config.get('max_retries', 30),
            'initial_retry_delay': schwab_config.get('initial_retry_delay', 1),
            'http_timeout': schwab_config.get('http_timeout', 10)
        }
    
    def reload(self):
        """
        Reload configuration from files.
        
        Useful for picking up configuration changes without restarting the application.
        """
        # Reload environment variables
        load_dotenv(find_dotenv(), override=True)
        
        # Reload YAML configuration
        self._yaml_config = self._load_yaml_config()
        
        # Clear cache
        self._cache.clear()


# Global configuration instance for easy access
_config_instance = None

def get_config() -> Config:
    """
    Get the global configuration instance.
    
    Returns:
        The global Config instance
    """
    global _config_instance
    if _config_instance is None:
        _config_instance = Config()
    return _config_instance


# Example usage and testing
if __name__ == "__main__":
    """
    Example usage of the configuration management system.
    """
    config = Config()
    
    print("=== Configuration Management Example ===")
    print(f"Application Name: {config.get('application.name')}")
    print(f"Timezone: {config.get('application.timezone')}")
    print(f"Data API Name: {config.get('application.api.data_name')}")
    print(f"Max Retries: {config.get('schwab_api.max_retries')}")
    print()
    
    print("=== Service Configuration ===")
    balance_config = config.get_service_config('balance_service')
    print(f"Balance Check Times: {balance_config.get('check_times')}")
    print(f"Balance Check Interval: {balance_config.get('check_interval')}s")
    print()
    
    print("=== Database Configuration ===")
    try:
        db_config = config.get_database_config()
        print(f"Database: {db_config['database']}")
        print(f"Port: {db_config['port']}")
        print(f"Driver: {db_config['driver']}")
    except ValueError as e:
        print(f"Database config error: {e}")
