from __future__ import print_function

import io
import logging
from pathlib import Path

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


class GoogleDriveClient:
    """A client service for interacting with Google Drive"""

    def __init__(
            self,
            config_manager,
            logger: logging.Logger
    ):
        """Initializes the GdriveService"""
        self._credentials = config_manager.get_credentials()
        self._service = build("drive", "v3", credentials=self._credentials)
        self.logger = logger

    @property
    def creds(self):
        return self._credentials

    @property
    def service(self):
        return self._service

    def _check_fetch_status(self, downloader_instance: MediaIoBaseDownload):
        """Check file download status"""
        done: bool = False
        while not done:
            status, done = downloader_instance.next_chunk()
            if status:
                self.logger.info(f"Download {int(status.progress() * 100)}.")
            return status

    # TODO: Change name to google workspace file
    def fetch_file_from_drive(
            self, file_id: str, mime_type: str, download_dir: Path, file_name: str
    ) -> None:
        """Download a Google Workspace file from Google Drive to 
        a different format"""
        request = self.service.files().export_media(
            fileId=file_id, mimeType=mime_type
        )
        file_object = io.FileIO(download_dir / file_name, "wb")
        downloader = MediaIoBaseDownload(file_object, request)

        self._check_fetch_status(downloader)

    def download_file_from_drive(self, file_id: str, download_dir: Path,
                                 file_name: str) -> None:
        """Download a file from Google Drive without conversion"""
        request = self.service.files().get_media(fileId=file_id)
        file_object = io.FileIO(download_dir / file_name, "wb")
        downloader = MediaIoBaseDownload(file_object, request)

        self._check_fetch_status(downloader)

    def retrieve_sheet_data(
            self,
            file_sheet_id: str,
            sheet_range: str,
    ) -> list | None:
        """Reads a Google Sheet and returns the data"""
        # Build sheet api service
        sheet_service = build("sheets", "v4", credentials=self._credentials)
        # Call the Sheets API
        result = (
            sheet_service.spreadsheets()
            .values()
            .get(spreadsheetId=file_sheet_id, range=sheet_range)
            .execute()
        )
        return result.get("values", [])
