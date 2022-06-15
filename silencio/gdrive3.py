import re
from csv import DictReader
from functools import partial
from io import BytesIO
from operator import attrgetter
from pathlib import Path
from typing import Callable, Iterator, Sequence, Optional, Mapping, Union

from cytoolz import first, keyfilter
from dustgoggles.pivot import split_on
import googleapiclient.discovery as discovery
import pandas as pd
from googleapiclient.errors import BatchError
from googleapiclient.http import MediaInMemoryUpload
from oauth2client.client import AssertionCredentials

from silencio.treeutils import (
    add_files_to_segmented_trees,
    flip_path_tree,
    make_drive_adjacency_list,
    segment_trees,
)


class DriveResource(discovery.Resource):
    about: Callable
    files: Callable
    new_batch_http_request: Callable


class DriveBot:
    def __init__(self, creds: AssertionCredentials):
        self.resource: DriveResource = discovery.build(
            "drive", "v3", credentials=creds
        )
        self.about = self.resource.about
        self.files = self.resource.files
        self.new_batch_http_request = self.resource.new_batch_http_request
        self.scanner = DriveScanner(self)
        self.filesystem = {}
        self.collisions = {}
        self.root_id = ""
        self.batches = []

    def scan(self, force=False, verbose=True):
        if (self.scanner.complete is True) and (force is False):
            return
        if force is True:
            self.scanner = DriveScanner(self)
        self.scanner.get(verbose)

    def set_filesystem(self, root_id=None):
        if self.scanner.complete is False:
            self.scan()
        (
            self.filesystem,
            self.collisions,
            self.root_id,
        ) = self.scanner.extract_filesystem(root_id)

    def ls(self, folder_name=None, folder_id=None, return_collisions=False):
        folder_name, folder_id = self._pick_name_id(folder_name, folder_id)
        if folder_id is not None:
            files, collisions, _ = DriveScanner(
                self, f"'{folder_id}' in parents"
            ).extract_filesystem()
        else:
            files = ls_fs_dict(folder_name, self.filesystem)
            collisions = None
            if return_collisions is True:
                collisions = ls_fs_dict(folder_name, self.collisions)
        if return_collisions is True:
            return files, collisions
        return files

    def find(self, folder_name=None, folder_id=None, regex=None):
        raise NotImplementedError

    @staticmethod
    def _decode_csv(text, to_pandas=True, **pd_kwargs):
        # inefficient, better to know sep from the outset...but whatever
        records = list(DictReader(re.split("[\r\n]+", text)))
        if to_pandas is True:
            return pd.DataFrame(records, **pd_kwargs)
        return records

    def _get_csv(
        self,
        get_method,
        name=None,
        file_id=None,
        to_pandas=True,
        defer=False,
        **pd_kwargs,
    ):
        file_id = self._pick_id(name, file_id)
        request = get_method(fileId=file_id)
        if defer is True:
            return request
        text = request.execute().decode()
        return self._decode_csv(text, to_pandas, **pd_kwargs)

    def read_sheet(self, name=None, file_id=None, to_pandas=True):
        """
        returns a google sheet as a dict or DataFrame. does not have all the
        functionality of the Sheets API. reads only the first sheet of a
        multi-sheet Sheet.
        """
        get_method = partial(self.files().export, mimeType="text/csv")
        return self._get_csv(get_method, name, file_id, to_pandas)

    def read_csv(self, name=None, file_id=None, to_pandas=True):
        """
        returns a csv file stored in Drive as a dict or DataFrame.
        """
        get_method = self.files().get_media
        return self._get_csv(get_method, name, file_id, to_pandas)

    def read_file(self, file_id, defer=False):
        request = self.files().get_media(fileId=file_id)
        if defer is True:
            return request
        return request.execute()

    # TODO: allow updating an existing file by path name
    def df_to_drive_csv(
        self,
        df: pd.DataFrame,
        name: Optional[str] = None,
        folder_name: Optional[str] = None,
        file_id: Optional[str] = None,
        folder_id: Optional[str] = None,
        defer: bool = False,
    ):
        """
        writes a DataFrame to Drive as a csv file.
        """
        name, file_id = self._pick_name_id(name, file_id)
        buffer = BytesIO()
        # noinspection PyTypeChecker
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        csv_body = MediaInMemoryUpload(buffer.read(), mimetype="text/csv")
        if name is not None:
            folder_id = self._pick_id(folder_name, folder_id)
            request = self.files().create(
                body={"name": name, "parents": [folder_id]},
                media_body=csv_body,
            )
        else:
            request = self.files().update(
                fileId=file_id, media_body=csv_body
            )
        if defer is True:
            return request
        return request.execute()

    def mv(
        self,
        name=None,
        target=None,
        defer=False
    ):
        parameters = {}
        if Path(target).is_file():
            parameters['addParents'] = self.filesystem[
                str(Path(target).parent)
            ]
            parameters['body'] = {}
            parameters['body']['name'] = str(Path(target).name)
        else:
            parameters['addParents'] = self.filesystem[target]
        parameters['removeParents'] = str(Path(self.filesystem[name]).parent)
        parameters['fileId'] = self.filesystem[name]
        request = self.files().update(**parameters)
        if defer is True:
            return request
        return request.execute()

    def rm(self, name=None, file_id=None, defer=False):
        file_id = self._pick_id(name, file_id)
        request = self.files().delete(fileId=file_id)
        if defer is True:
            return request
        return request.execute()

    def add_request(self, request):
        if len(self.batches) == 0:
            self.batches.append(self.new_batch_http_request())
        try:
            self.batches[-1].add(request)
        except BatchError:
            self.batches.append(self.new_batch_http_request())
            self.add_request(request)

    def execute_batches(self, clear_batches=True):
        execution = tuple(
            map(lambda x: x(), map(attrgetter("execute"), self.batches))
        )
        if clear_batches:
            self.batches = []
        return execution

    def name_to_id(self, name):
        return first(keyfilter(lambda k: k == name, self.filesystem).values())

    def _pick_id(self, name=None, file_id=None):
        name, file_id = self._pick_name_id(name, file_id)
        if name is not None:
            file_id = self.name_to_id(name)
        return file_id

    def _pick_name_id(self, name=None, file_id=None):
        if (name is None) and (file_id is None):
            raise ValueError("Must provide either file_id or a new file name.")
        if (name is not None) and (file_id is not None):
            raise ValueError("Will not both update and create a new file.")
        if file_id is None and self.filesystem is None:
            raise ValueError(
                "A filesystem must be initialized for this DriveBot before "
                "executing methods using file paths rather than raw file ids."
                "Try DriveBot.set_filesystem()."
            )
        return name, file_id


class DriveScanner(Iterator):
    def __init__(
        self,
        drivebot: DriveBot,
        query: str = "trashed=false",
        fields: Sequence[str] = (
            "kind",
            "id",
            "name",
            "mimeType",
            "parents",
            "size",
            "owners",
            "modifiedTime",
            "md5Checksum",
        ),
        page_size: int = 1000,
    ):
        self.drivebot = drivebot
        self.results = []
        self.page_token = None
        self.page_counter = 0
        self.page_size = page_size
        self.query = query
        self.fields = fields
        self.response = None
        self.is_queried = False
        self.complete = False
        self.directories = None
        self.files = None
        self.trees = {}

    def get(self, verbose: bool = False):
        if verbose is True:
            print("fetching...", end="")
        while self.complete is False:
            if verbose is True:
                print(".", end="")
            next(self)
        return self.results

    def compose_request(self) -> dict[str]:
        return {
            "q": self.query,
            "fields": f"nextPageToken, files({','.join(self.fields)})",
            "pageToken": self.page_token,
        }

    def make_manifest(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        manifest = pd.DataFrame.from_records(self.results)
        manifest = manifest.dropna(subset=["parents"])
        manifest["parents"] = manifest["parents"].str.join("")
        directory_ids = manifest["parents"].unique()
        self.directories, self.files = split_on(
            manifest, manifest["id"].isin(directory_ids)
        )
        return self.directories, self.files

    def get_file_trees(self):
        if (self.directories is None) and (self.files is None):
            self.make_manifest()
        adjacencies = make_drive_adjacency_list(self.directories)
        segments = segment_trees(adjacencies)
        self.trees = add_files_to_segmented_trees(segments, self.files)
        return self.trees

    def extract_filesystem(self, root_id=None):
        if len(self.trees) == 0:
            self.get_file_trees()
        if root_id is None:
            root_id = list(self.trees.keys())[0]
        filesystem, collisions = flip_path_tree(self.trees[root_id])
        return filesystem, collisions, root_id

    def __next__(self):
        if self.complete:
            raise StopIteration
        self.is_queried = True
        self.response = (
            self.drivebot.files().list(**self.compose_request()).execute()
        )
        self.page_token = self.response.get("nextPageToken")
        if self.page_token is None:
            self.complete = True
        files = self.response.get("files", {})
        self.results += files
        self.page_counter += 1
        return files

    def __repr__(self):
        description = f"Drive file list, "
        if self.is_queried is False:
            description += "not yet queried"
        else:
            description += f"{len(self.results)} retrieved files, "
        if self.complete is True:
            description += "retrieval complete"
        elif self.is_queried is True:
            description += f"{self.page_counter} pages retrieved"
        return description

    def __str__(self):
        return self.__repr__()


def ls_fs_dict(folder_name, fs_dict):
    return keyfilter(lambda fn: str(Path(fn).parent) == folder_name, fs_dict)


def copy_gdrive(
    bot: DriveBot,
    drive_folder_id: str,
    patterns: Mapping[str, Union[str, re.Pattern]],
    settings: Mapping[str, str],
    current_path: Path = Path("."),
) -> list[Path]:
    contents = bot.ls(folder_id=drive_folder_id)
    # folders, files = separate_by(contents, is_drive_folder)
    # matcher = partial(match_drivefile_title, pattern=patterns["file"])
    # matches = list(filter(matcher, files))
    # saved_files = []
    # local_path = Path(settings["local_root"], current_path)
    # if any(matches):
    #     os.makedirs(local_path, exist_ok=True)
    # filenames, groups = group_files(matches)
    # # doing this to filter duplicates -- the google drive "filesystem"
    # # (it's not) is allowed duplicate "titles", but our local filesystem of
    # # course is not
    # for fn, group in zip(filenames, groups):
    #     saved_files += process_match_group(fn, group, current_path, settings)
    # if local_path.exists() and (settings["remove_extras"] is True):
    #     delete_extras(filenames, local_path)
    # for folder in folders:
    #     path = Path(current_path, folder["title"])
    #     if "folder" in patterns.keys():
    #         if not re.match(patterns["folder"], str(path)):
    #             continue
    #     saved_files += copy_gdrive(bot, folder["id"], patterns, settings, path)
    # return saved_files
