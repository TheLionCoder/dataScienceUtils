import io
from pathlib import Path
from typing import Dict, List, Mapping, Optional

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload


class GoogleDriveClient:
    """
    - The Google Drive Client is client service for interacting with
      Google Drive services.
    - The Google Drive Client is used to download files from Google Drive.
    - The Google Drive Client is used to download Google Sheets data.
    - The class requires a `credentials' parameter which is a Google OAuth2
      credentials object, a
    `service` parameter which is a Google Drive service object, and a `logger`
      parameter, which is a
    logging.Logger object.
    - The class provides a property 'creds' which returns the Google OAuth2
      credentials object and a
      property 'service' which returns the Google Drive service object.
    - The `create_file_writer` method is used to create a file writer.
    - The `send_download_request` method is used to send a download request.
    - The `track_download_progress` method is used to track the progress of a
      file download.
    - The `track_download_progress` method is used to track the progress of a
      file download.
    - The `fetch_file_from_google_workspace` method is used to download a
      Google Workspace file from
    Google Drive to a different format.
    - The `download_file_without_conversion` method is used to download a file
      from Google Drive without
    a conversion.
    - The `retrieve_sheet_data` method is used to read a Google Sheet and
      return the data.
    """

    def __init__(self, drive_config_manager):
        """Initializes the GdriveService
        :param drive_config_manager: A ConfigManager object.
        """
        self._credentials = drive_config_manager.get_credentials()
        self._service = build("drive", "v3", credentials=self._credentials)
        self._sheet_service = build("sheets", "v4", credentials=self._credentials)

    @property
    def creds(self):
        return self._credentials

    @property
    def service(self):
        return self._service

    @staticmethod
    def _create_file_writer(download_path: Path) -> io.FileIO:
        """Make a file writer
        :param download_path: The directory to download the file to.

        :return: A file object to write to the downloaded file.
        """
        return io.FileIO(download_path, mode="wb")

    @staticmethod
    def _send_download_request(
        file_writer: io.FileIO | io.BytesIO, export_request: dict
    ) -> MediaIoBaseDownload:
        """Make a download request
        :param file_writer: A file object to write to the downloaded file.
        :param export_request: A dictionary containing the file.
        :return: A MediaIoBaseDownload object.
        """
        return MediaIoBaseDownload(file_writer, export_request)

    @staticmethod
    def _prepare_file_metadata(
        file_name: str, *, folder_id: List[str], **kwargs
    ) -> Dict[str, str | List[str]]:
        """Prepare file metadata for upload file
        :param file_name: The name of the file to upload.
        :param mimetype: The mimetype of the file to upload.
        :param folder_id: The id of the folder to upload the file to.
        :param kwargs: Additional metadata to add to the file.
        :return: A dictionary containing the file metadata.
        """
        file_metadata: Dict[str, str | List[str]] = {
            "name": file_name,
            "parents": folder_id,
        }
        # Update metadata dict with kwargs
        file_metadata.update(kwargs)
        return file_metadata

    @staticmethod
    def track_download_progress(
        downloader_instance: MediaIoBaseDownload,
    ) -> MediaIoBaseDownload:
        """Check file download status
        :param downloader_instance: A MediaIoBaseDownload object.
        :return: The status of the download.
        """
        done: bool = False
        status = None
        while not done:
            status, done = downloader_instance.next_chunk()
        return status

    def _send_upload_request(
        self, file_metadata: Mapping, mimetype: str, file_path: str
    ) -> Dict[str, str]:
        """Make and upload request
        :param file_metadata: A dictionary containing the file metadata.
        :param mimetype: The mimetype of the file to upload
        :file_path: The path of the file to upload.
        :return: A dictionary containing the file id.
        """
        media = MediaFileUpload(file_path, mimetype=mimetype, resumable=True)
        return (
            self.service.files()
            .create(body=file_metadata, media_body=media, fields="id")
            .execute()
        )

    def download_file(
        self,
        download_file_path: Path,
        *,
        file_id: str,
        mime_type: Optional[str] = None,
    ) -> None:
        """Download a file from Google Drive
        :param file_id: The id of the file to download.
        :param download_file_path: The directory to download the file to.
        :param mime_type: The mime type of the file to download. Default is
         None.
        see: https://developers.google.com/drive/api/guides/ref-export-formats
        """
        try:
            if mime_type:
                export_request = self.service.files().export_media(
                    fileId=file_id, mimeType=mime_type
                )
            else:
                export_request = self.service.files().get_media(fileId=file_id)
            file_writer = GoogleDriveClient._create_file_writer(download_file_path)
            download_request_response = GoogleDriveClient._send_download_request(
                file_writer, export_request
            )
            self.track_download_progress(download_request_response)
        except Exception as e:
            raise e

    def retrieve_small_file_data(self, file_id: str) -> io.BytesIO:
        """ "
        Retrieve a small file from Google Drive as a BytesIO object.
        :param file_id: The id of the file to download.
        :return: The file content."""
        request = self.service.files().get_media(fileId=file_id)
        file_content = io.BytesIO()
        downloader_request_response = GoogleDriveClient._send_download_request(
            file_content, request
        )
        self.track_download_progress(downloader_request_response)
        file_content.seek(0)
        return file_content

    def retrieve_sheet_data(
        self,
        file_id: str,
        sheet_range: str,
    ) -> list | None:
        """Reads a Google Sheet and returns the data
        :param file_id: The file_id  of the Google Sheet to read.
        :param sheet_range: The range of the Google Sheet to read
        :Return The data from the Google Sheet.
        """
        # Call the Sheets API
        result = (
            self._sheet_service.spreadsheets()
            .values()
            .get(spreadsheetId=file_id, range=sheet_range)
            .execute()
        )
        return result.get("values", [])

    def upload_file(
        self, file_path: str, file_name: str, mimetype: str, folder_id: str, **kwargs
    ) -> None:
        """Upload a file to Google Drive
        :param file_path: The path of the file to upload.
        :param file_name: The name of the file to upload.
        :param mimetype: The mimetype of the file to upload.
        :param folder_id: The Google drive id of the folder to upload the file to.
        :param kwargs: Additional metadata to add to the file.
        """
        folder_id: List[str] = [folder_id]
        file_metadata: Mapping = GoogleDriveClient._prepare_file_metadata(
            file_name, folder_id=folder_id, **kwargs
        )
        self._send_upload_request(file_metadata, mimetype, file_path)
