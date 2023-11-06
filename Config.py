# *-* encoding: utf-8 *-*
from pathlib import Path
from typing import Any, Union

import yaml


class Config:
    """
    Config class

    Handles loading and accessing properties from YAML config files.

    Attributes:
        config: The loaded configuration from the config file.

    Methods:
        __init__: Initializes the Config object.
        _load_config: Loads the configuration from the config file.
        _try_get_key: Tries to retrieve a value from a dictionary based on a key.
        get_property: Retrieves a property from the loaded configuration.
    """
    def __init__(self, config_file: Union[str, Path]):
        self.config = self._load_config(config_file)

    @staticmethod
    def _load_config(config_file: Union[str, Path]) -> dict[str, Any]:
        try:
            with open(config_file, "r") as stream:
                return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            raise ValueError(f"Error while loading config file: {exc}")

    @staticmethod
    def _try_get_key(dictionary: dict[str, Any], key: str) -> Any:
        try:
            return dictionary[key]
        except KeyError:
            raise ValueError(f"Key {key} not found in config file.")

    def get_property(self, *keys) -> Any:
        value = self.config
        for key in keys:
            value = self._try_get_key(value, key)
            if value is None:
                raise ValueError(f"Key {key} not found in config file.")
        return value
