# *-* encoding: utf-8 *-*
from pathlib import Path
from typing import Any

import yaml


class YamlConfigManager:
    """
    YamlConfigManager class

    Handles loading and accessing properties from YAML config files.

    Attributes:
        _config_file: The absolute path to the config file.
        _config: The loaded configuration from the config file.

    Methods:
        __init__: Initializes the Config object.
        _load_config: Loads the configuration from the config file.
        get_property: Retrieves a property from the loaded configuration.
    """

    def __init__(self, file_path: Path):
        assert isinstance(file_path, Path), "file_path must be a Path object."
        self._config_file: Path = file_path
        self._config = self._load_config()

    def _load_config(self) -> dict[str, Any]:
        """Loads the configuration from the config file."""
        try:
            with open(self._config_file, "r") as stream:
                return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            raise ValueError(f"Error while loading config file: {exc}")

    def get_property(self, *keys) -> Any:
        """Gets a property from the loaded configuration.
        :param keys: The keys to access the property."""
        value = self._config
        for key in keys:
            try:
                value = value[key]
            except KeyError:
                raise ValueError(f"Key {key} not found in config file.")
        return value
