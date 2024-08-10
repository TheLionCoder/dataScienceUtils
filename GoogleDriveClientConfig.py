from pathlib import Path
from typing import Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow


class GoogleDriveClientConfig:
    """
    - The `GoogleDriveClientConfig` class is manager for the configuration
    file of the GoogleDriveClient class
    - The class requires a `token_file_path`, a
      `credential_file_path` parameters
    which should be Path objects and `logger` parameter.
    - The `retrieve_credentials` method retrieves the credentials from the
      token file.
    - The `_credentials_expired` method checks if the credentials are expired.
    - The `_refresh_credentials` method refreshes the credentials.
    - The `update_token_file` method updates the token file with the
      new credentials.
    - The `get_credentials` method gets the credentials from the token file
      or gets new ones.
    - The `get_credentials_from_flow` method gets the credentials from
    the flow.
    """

    def __init__(
        self,
        config_dir_path: Path,
        scope: list,
    ):
        """Constructor for the GoogleDriveClientConfig
        :param config_dir_path: Path to the configuration directory
         where the token and credentials files are stored.
        :param scope: Scope of the token.
        """
        assert config_dir_path.is_dir(), "The config path should be a directory"
        self._conf_path: Path = config_dir_path
        self._token_file_path = self._conf_path.joinpath("google_token.json")
        self._credential_file_path = self._conf_path.joinpath(
            "google_credentials.json"
        ).as_posix()
        self._scope = scope

    def __str__(self):
        """String representation of the GoogleDriveClientConfig"""
        return (
            f"GoogleDriveClientConfig(token_file={self._token_file_path},"
            f"credential_file={self._credential_file_path}, "
            f"scope={self._scope}"
        )

    def retrieve_credentials(self) -> Credentials:
        """Gets the credentials from the token file."""
        if self._token_file_path.exists():
            return Credentials.from_authorized_user_file(
                self._token_file_path.as_posix(), self._scope
            )

    @staticmethod
    def _check_credentials_expire(creds: Optional[Credentials]) -> bool | None:
        """Checks if the credentials are expired"""
        return creds and creds.expired and creds.refresh_token

    @staticmethod
    def _refresh_credentials(creds: Credentials) -> None:
        """Refreshes the credentials"""
        creds.refresh(Request())

    def update_token_file(self, credentials: Credentials) -> None:
        """Updates the token file with the new credentials"""
        with self._token_file_path.open("w") as token:
            token.write(credentials.to_json())

    def get_credentials(self) -> Credentials:
        """Gets the credentials from the token file or get new ones"""
        creds = self.retrieve_credentials()

        if creds and creds.valid:
            return creds

        if creds and self._check_credentials_expire(creds):
            self._refresh_credentials(creds)

        return self.get_credentials_from_flow()

    def get_credentials_from_flow(self) -> Credentials:
        """Gets the credentials from the flow"""
        flow = InstalledAppFlow.from_client_secrets_file(
            self._credential_file_path, self._scope
        )
        creds = flow.run_local_server(port=0)
        self.update_token_file(creds)
        return creds
