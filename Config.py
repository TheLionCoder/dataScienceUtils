# *-* encoding: utf-8 *-*
from pathlib import Path
from typing import Any

import yaml


class Config:
    """
    Config class

    Handles loading and accessing properties from YAML config files.

    Attributes:
        _current_path: The absolute path to the current directory.
        _config_file: The absolute path to the config file.
        _config: The loaded configuration from the config file.

    Methods:
        __init__: Initializes the Config object.
        _load_config: Loads the configuration from the config file.
        get_property: Retrieves a property from the loaded configuration.
    """

    def __init__(self, file_name: str):
        self._current_path: Path = Path(__file__).parent.absolute()
        self._config_file: Path = self._current_path.parents[1].joinpath(
            "conf", "base", file_name
        )
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
