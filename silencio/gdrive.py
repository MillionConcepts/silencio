import datetime as dt
from functools import partial
from itertools import groupby
import os
from pathlib import Path
import re
from typing import Union, Optional, Sequence, Any, Mapping

from pydrive2.files import GoogleDriveFile
from typing_extensions import TypeAlias

import dateutil.parser as dtp
from dustgoggles.structures import separate_by
from pydrive2.drive import GoogleDrive

Pathlike: TypeAlias = Union[str, Path]
GoogleDriveId: TypeAlias = str


def is_drive_folder(drive_file: GoogleDriveFile) -> bool:
    return drive_file["mimeType"] == "application/vnd.google-apps.folder"


class DriveBot(GoogleDrive):
    """
    convenience wrapper adding abstract pseudo-filesystem operations to
    a pydrive2 GoogleDrive object
    """

    # TODO: maybe consider doing this with one of the fs contrib things
    #  instead? so you can have a single gdrive / s3 interface? or perhaps
    #  combining it in some way with google sheets, maybe even dropping to
    #  lower-level API functions? or perhaps not.
    def mkdir(
        self, folder_name: str, parent_id: GoogleDriveId
    ) -> GoogleDriveId:
        gdrive_folder = self.CreateFile(
            {
                "title": folder_name,
                "parents": [{"id": parent_id}],
                "mimeType": "application/vnd.google-apps.folder",
            }
        )
        gdrive_folder.Upload()
        folder_id = gdrive_folder["id"]
        return folder_id

    def find(
        self,
        pattern: Union[str, re.Pattern] = None,
        root_path: GoogleDriveId = None,
        verbose: bool = False,
    ) -> list[GoogleDriveFile]:
        # faster...but everything.
        print("ROOT: " + root_path)
        if pattern is None:
            query = ""
        else:
            query = f"title contains '{pattern}' and "
        if root_path is None:
            result = self.ListFile({"q": f"{query}trashed=false"}).GetList()
            return list(
                filter(lambda file: re.match(pattern, file["title"]), result)
            )
        # TODO: this should always work, but may be excessively slow due
        #  to the number of distinct calls to the Drive API. I'm not sure
        #  how to do it more quickly, though.
        contents = self.ls(root_path)

        def matcher(drivefile: GoogleDriveFile):
            if pattern is None:
                return True
            return re.match(pattern, drivefile["title"])

        matches = list(filter(matcher, contents))
        if verbose and matches:
            list(map(lambda x: print(x["title"]), matches))
        for folder in filter(is_drive_folder, contents):
            matches += self.find(pattern, folder["id"], verbose)
        return matches

    def cp(self, source_path: Pathlike, target_folder: str):
        upload = self.CreateFile(
            {
                "title": Path(source_path).name,
                "parents": [{"id": target_folder}],
            }
        )
        upload.SetContentFile(source_path)
        upload.Upload()

    def ls(self, folder_id: GoogleDriveId) -> list[GoogleDriveFile]:
        filelist = self.ListFile({"q": f"'{folder_id}' in parents"}).GetList()
        return filelist

    def get_checksums(
        self,
        folder_id: GoogleDriveId,
        file_list: Optional[Sequence[Any]] = None,
    ) -> dict[str, str]:
        if file_list is None:
            file_list = self.ls(folder_id)
        return {
            file.get("title"): file.get("md5Checksum") for file in file_list
        }

    def cd(self, folder_name: str, parent_id: GoogleDriveId) -> GoogleDriveId:
        root_filelist = self.ls(parent_id)
        folder_list = [
            file for file in root_filelist if file["title"] == folder_name
        ]
        if folder_list:
            folder_id = folder_list[0]["id"]
        else:
            folder_id = self.mkdir(folder_name, parent_id)
        return folder_id


def match_drivefile_title(
    drivefile: GoogleDriveFile, pattern: Union[str, re.Pattern]
):
    if pattern is None:
        return True
    return re.match(pattern, drivefile["title"])


def stamp() -> str:
    return dt.datetime.utcnow().isoformat()[:19]


def process_match_group(
    fn: str,
    files: Sequence[GoogleDriveFile],
    current_path: Pathlike,
    settings: Mapping,
) -> list:
    if len(files) > 1:
        message = (
            f"{stamp()} refusing to sync duplicates of {current_path}/{fn}"
        )
        print(message)
        with open(settings["logfile"], "a+") as logfile:
            logfile.write(message)
        return []

    local = Path(settings["local_root"], current_path, fn)
    if (settings.get("newer_only") is True) and local.exists():
        local_mtime = dt.datetime.fromtimestamp(
            os.path.getmtime(local), tz=dt.timezone.utc
        )
        remote_mtime = dtp.parse(files[0]["modifiedDate"])
        if local_mtime > remote_mtime:
            if settings.get("verbose") is True:
                print(f"skipping older version of {current_path}/{fn}")
            return []
    if settings.get("verbose") is True:
        print(f"copying {current_path}/{fn} to local")
    files[0].GetContentFile(str(local))
    return [local]


def copy_gdrive(
    bot,
    drive_folder_id: str,
    patterns: Mapping[str, Union[str, re.Pattern]],
    settings: Mapping[str, str],
    current_path: Path = Path("."),
) -> list[Path]:
    contents = bot.ls(drive_folder_id)
    folders, files = separate_by(contents, is_drive_folder)
    matcher = partial(match_drivefile_title, pattern=patterns["file"])
    matches = list(filter(matcher, files))
    saved_files = []
    if any(matches):
        os.makedirs(Path(settings["local_root"], current_path), exist_ok=True)
    for fn, grouper in groupby(matches, lambda m: m["title"]):
        saved_files += process_match_group(
            fn, list(grouper), current_path, settings
        )
    for folder in folders:
        path = Path(current_path, folder["title"])
        if "folder" in patterns.keys():
            if not re.match(patterns["folder"], str(path)):
                continue
        saved_files += copy_gdrive(bot, folder["id"], patterns, settings, path)
    return saved_files
