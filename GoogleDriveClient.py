import io
import logging
from pathlib import Path
from urllib.parse import urlparse, ParseResult

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


class GoogleDriveClient:
    """
    - The Google Drive Client is client service for interacting with Google Drive services.
    - The Google Drive Client is used to download files from Google Drive.
    - The Google Drive Client is used to download Google Sheets data.
    - The class requires a `credentials' parameter which is a Google OAuth2 credentials object, a
    `service` parameter which is a Google Drive service object, and a `logger` parameter, which is a
    logging.Logger object.
    - The class provides a property 'creds' which returns the Google OAuth2 credentials object and a
    property 'service' which returns the Google Drive service object.
    - The `create_file_writer` method is used to create a file writer.
    - The `send_download_request` method is used to send a download request.
    - The `track_download_progress` method is used to track the progress of a file download.
    - The `track_download_progress` method is used to track the progress of a file download.
    - The `fetch_file_from_google_workspace` method is used to download a Google Workspace file from
    Google Drive to a different format.
    - The `download_file_without_conversion` method is used to download a file from Google Drive without
    a conversion.
    - The `retrieve_sheet_data` method is used to read a Google Sheet and return the data.
    """

    def __init__(self, config_manager, logger: logging.Logger):
        """Initializes the GdriveService
        :param config_manager: A ConfigManager object.
        :param logger: A logging.Logger object.
        """
        self._credentials = config_manager.get_credentials()
        self._service = build("drive", "v3", credentials=self._credentials)
        self._sheet_service = build("sheets", "v4", credentials=self._credentials)
        self.logger = logger

    @property
    def creds(self):
        return self._credentials

    @property
    def service(self):
        return self._service

    @staticmethod
    def __retrieve_file_id(url: str) -> str | None:
        """Extract the file id from a Google Drive url
        :param url: The url to extract the file id from.
        :return: The file id or None.
        """
        parsed_url: ParseResult = urlparse(url)
        path_segments: list[str] = parsed_url.path.split("/")
        file_id: str = path_segments[3] if len(path_segments) > 3 else None
        return file_id

    @staticmethod
    def __create_file_writer(download_dir: Path, file_name: str) -> io.FileIO:
        """Make a file writer
        :param download_dir: The directory to download the file to.
        :param file_name: The name of the file to download.
        :return: A file object to write to the downloaded file.
        """
        download_path: Path = download_dir / file_name
        return io.FileIO(download_path, mode="wb")

    @staticmethod
    def __send_download_request(
        file_writer: io.FileIO, export_request: dict
    ) -> MediaIoBaseDownload:
        """Make a download request
        :param file_writer: A file object to write to the downloaded file.
        :param export_request: A dictionary containing the file.
        :return: A MediaIoBaseDownload object.
        """
        return MediaIoBaseDownload(file_writer, export_request)

    def track_download_progress(self, downloader_instance: MediaIoBaseDownload
                                ) -> MediaIoBaseDownload:
        """Check file download status
        :param downloader_instance: A MediaIoBaseDownload object.
        :return: The status of the download.
        """
        done: bool = False
        status = None
        while not done:
            status, done = downloader_instance.next_chunk()
            if status:
                self.logger.info(f"\033[95mDownload {int(status.progress() * 100)}%.\033[0m")
        return status

    def fetch_file_from_google_workspace(
        self, file_url: str, mime_type: str, download_dir: Path, file_name: str
    ) -> None:
        """Download a Google Workspace file from Google Drive to
        a different format
        :param file_url: The url of the file to download.
        :param mime_type: The mime type of the file to download.
        see: https://developers.google.com/drive/api/guides/ref-export-formats
        :param download_dir: The directory to download the file to.
        :param file_name: The name of the file to download.
        """
        file_id: str = GoogleDriveClient.__retrieve_file_id(file_url)
        export_request = self.service.files().export_media(
            fileId=file_id, mimeType=mime_type
        )
        file_writer = GoogleDriveClient.__create_file_writer(download_dir, file_name)
        download_request_response = GoogleDriveClient.__send_download_request(
            file_writer, export_request
        )
        self.track_download_progress(download_request_response)

    def download_file_without_conversion(
        self, file_url: str, download_dir: Path, file_name: str
    ) -> None:
        """Download a file from Google Drive without a conversion
        :param file_url: The url of the file to download.
        :param download_dir: The directory to download the file to.
        :param file_name: The name of the file to download.
        """
        file_id: str = GoogleDriveClient.__retrieve_file_id(file_url)
        request = self.service.files().get_media(fileId=file_id)
        file_writer = GoogleDriveClient.__create_file_writer(download_dir, file_name)
        download_request_response = GoogleDriveClient.__send_download_request(file_writer, request)
        self.track_download_progress(download_request_response)

    def retrieve_sheet_data(
        self,
        file_sheet_url: str,
        sheet_range: str,
    ) -> list | None:
        """Reads a Google Sheet and returns the data
        :param file_sheet_url: The url of the Google Sheet to read.
        :param sheet_range: The range of the Google Sheet to read.
        :return The data from the Google Sheet.
        """
        file_sheet_id: str = GoogleDriveClient.__retrieve_file_id(file_sheet_url)
        # Call the Sheets API
        result = (
            self._sheet_service.spreadsheets()
            .values()
            .get(spreadsheetId=file_sheet_id, range=sheet_range)
            .execute()
        )
        return result.get("values", [])
