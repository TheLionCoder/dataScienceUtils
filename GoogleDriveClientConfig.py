from __future__ import print_function

from pathlib import Path
from typing import Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow


class GoogleDriveClientConfig:
    """
    - The `GoogleDriveCLienteConfig` class is manager for the configuration
    file of the GoogleDriveClient class
    - The class requires a `token_file_path`, a `credential_file_path` parameters
    wich should be Path objects and `logger` parameter.
    - The `get_credent

    """

    def __init__(
            self,
            token_file_path: Path,
            credential_file_path: Path,
            scope: list,
    ):
        """Constructor for the GoogleDriveClientConfig
        :param token_file_path: Path to the token file.
        :param credential_file_path: Path to the credential file.
        :param scope: Scope of the token.
        """
        self.token_file_path = token_file_path
        self.credential_file_path = credential_file_path
        self.scope = scope

    def __str__(self):
        """String representation of the GoogleDriveClientConfig"""
        return (
            f"GoogleDriveClientConfig(toke_file={self.token_file_path},"
            f"credential_file={self.credential_file_path}, "
            f"scope={self.scope}"
        )

    def get_credentials_from_token_from_file(self) -> Credentials:
        """Gets the credentials from the token file or get new ones"""
        if self.token_file_path.exists():
            return Credentials.from_authorized_user_file(
                str(self.token_file_path), self.scope
            )

    @staticmethod
    def _credentials_expired(creds: Optional[Credentials]) -> bool:
        """Checks if the credentials are expired"""
        return creds and creds.expired and creds.refresh_token

    @staticmethod
    def _refresh_credentials(creds: Credentials) -> None:
        """Refreshes the credentials"""
        creds.refresh(Request())

    def update_token_file(self, credentials: Credentials) -> None:
        """Updates the token file with the new credentials"""
        with self.token_file_path.open("w") as token:
            token.write(credentials.to_json())

    def get_credentials(self) -> Credentials:
        """Gets the credentials from the token file or get new ones"""
        creds: Credentials = self.get_credentials_from_token_from_file()
        if not creds or not creds.valid:
            if self._credentials_expired(creds):
                self._refresh_credentials(creds)
        else:
            creds = self.get_credentials_from_flow()
        return creds

    def get_credentials_from_flow(self) -> Credentials:
        """Gets the credentials from the flow"""
        flow = InstalledAppFlow.from_client_secrets_file(
            str(self.credential_file_path), self.scope
        )
        creds = flow.run_local_server(port=0)
        self.update_token_file(creds)
        return creds
