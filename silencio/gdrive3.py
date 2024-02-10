from csv import DictReader
from functools import cache, partial
from io import BytesIO
import json
from operator import attrgetter
from pathlib import Path
import re
from typing import Callable, Iterator, Optional, Sequence, Union

from cytoolz import first, keyfilter
from dustgoggles.pivot import split_on
import googleapiclient.discovery as discovery
from googleapiclient.errors import BatchError
from googleapiclient.http import MediaInMemoryUpload, MediaFileUpload
from oauth2client.client import AssertionCredentials
import pandas as pd
import sh

from silencio.treeutils import (
    add_files_to_segmented_trees,
    flip_path_tree,
    make_drive_adjacency_list,
    segment_trees,
)


MIMETYPE = re.compile(r"\s+(\w+/\w+)\n")


def infer_mimetype(path: Union[str, Path]) -> str:
    try:
        return re.search(MIMETYPE, sh.file(str(path), mime_type=True)).group(1)
    except AttributeError:
        return "application/octet-stream"


class ExecutionError(Exception):
    """executing a batch failed."""
    pass


class DriveResource(discovery.Resource):
    about: Callable
    files: Callable
    new_batch_http_request: Callable


class DriveBot:
    def __init__(self, creds: AssertionCredentials, shared_drive_id=None):
        self.resource: DriveResource = discovery.build(
            "drive", "v3", credentials=creds
        )
        self.about = self.resource.about
        self.files = self.resource.files
        self.new_batch_http_request = self.resource.new_batch_http_request
        self.filesystem = {}
        self.collisions = {}
        self.root_id = ""
        self.batches = []
        self.shared_drive_id = shared_drive_id
        if self.shared_drive_id is not None:
            self.extra_parameters = {"supportsAllDrives": True}
        else:
            self.extra_parameters = {}
        self.errors = []
        self.scanner = DriveScanner(self)
        self.temp_filesystem = {}

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

    def mkdir(self, name, parent_name=None, parent_id=None, defer=False):
        request = self.files().create(
            body={
                "name": name,
                "parents": [self._pick_id(parent_name, parent_id)],
                "mimeType": "application/vnd.google-apps.folder",
            },
            **self.extra_parameters,
            fields="id"
        )
        if defer is False:
            return request.execute()
        return request

    @cache
    def cd(self, parent_id, name, mkdir=True):
        existing = self.ls(folder_id=parent_id)
        # TODO: check if this is actually a folder
        if name in existing.keys():
            return existing[name]
        if mkdir is True:
            return self.mkdir(name, parent_id=parent_id)['id']
        raise FileNotFoundError(
            f"{name} does not exist in {parent_id} & mkdir is False"
        )

    def manifest(self, folder_id, fields=None):
        kwargs = {'query': f"'{folder_id}' in parents and trashed=false"}
        if fields is not None:
            kwargs['fields'] = fields
        scanner = DriveScanner(self, **kwargs)
        scanner.get()
        return scanner.make_manifest()

    def ls(self, folder_name=None, folder_id=None):
        folder_name, folder_id = self._pick_name_id(folder_name, folder_id)
        if folder_id is not None:
            scanner = DriveScanner(
                self,
                f"'{folder_id}' in parents and trashed=false",
                fields=("name", "id")
            )
            scanner.get()
            return {f['name']: f['id'] for f in scanner.results}
        return ls_fs_dict(folder_name, self.filesystem)

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

    def get(self, file_id, target_file):
        with open(target_file, "wb") as stream:
            stream.write(self.read_file(file_id))

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
            request = self.files().update(fileId=file_id, media_body=csv_body)
        if defer is True:
            return request
        return request.execute()

    def put(
        self,
        source: Union[str, Path],
        folder_name: Optional[str] = None,
        folder_id: Optional[str] = None,
        defer: bool = False,
        mimetype: Optional[str] = None,
    ):
        folder_id = self._pick_id(folder_name, folder_id)
        mimetype = mimetype if mimetype is not None else infer_mimetype(source)
        request = self.files().create(
            body={"name": Path(source).name, "parents": [folder_id]},
            media_body=MediaFileUpload(source, mimetype=mimetype),
            **self.extra_parameters,
        )
        if defer is True:
            return request
        return request.execute()

    def mv(
        self,
        name: Optional[str] = None,
        file_id: Optional[str] = None,
        folder_name: Optional[str] = None,
        folder_id: Optional[str] = None,
        defer: bool = False,
    ):
        file_id = self._pick_id(name, file_id)
        folder_id = self._pick_id(folder_name, folder_id)
        parameters = {"addParents": folder_id, "fileId": file_id}
        if name is not None:
            try:
                parameters["removeParents"] = str(
                    Path(self.filesystem[name]).parent
                )
            except KeyError:
                pass
        if "removeParents" not in parameters:
            request = self.files().get(
                fileId=file_id, fields="parents", **self.extra_parameters
            )
            parameters["removeParents"] = request.execute()["parents"][0]
        request = self.files().update(**parameters, **self.extra_parameters)
        if defer is True:
            return request
        return request.execute()

    def cp(
        self,
        filename: str,
        path_name: Optional[str] = None,
        file_id: Optional[str] = None,
        target_folder_name: Optional[str] = None,
        target_folder_id: Optional[str] = None,
        defer: bool = False,
    ):
        file_id = self._pick_id(path_name, file_id)
        target_folder_id = self._pick_id(target_folder_name, target_folder_id)
        copy_body = {'name': filename, 'parents': [target_folder_id]}
        request = self.files().copy(
            fileId=file_id, body=copy_body, **self.extra_parameters
        )
        if defer is True:
            return request
        return request.execute()

    def get_checksums(self, folder_id, files=None):
        manifest = self.manifest(
            folder_id,
            ('name', 'id', 'md5Checksum', 'parents', 'mimeType', 'createdTime')
        )
        if len(manifest) == 0:
            return {}
        files = manifest['name'].tolist() if files is None else files
        checksums = {}
        for f in manifest.to_dict('records'):
            if (name := f['name']) not in files:
                continue
            # if someone has thrown a Google Workspace object in the folder,
            # it won't have a checksum, and we never, ever care about it
            # in a situation where we are producing checksums
            if (checksum := f.get('md5Checksum')) is None:
                continue
            checksums[name] = {
                'id': f['id'],
                'md5': f.get('md5Checksum'),
                'created': f['createdTime']
            }
        return checksums

    def rm(self, name=None, file_id=None, defer=False):
        file_id = self._pick_id(name, file_id)
        request = self.files().delete(fileId=file_id, **self.extra_parameters)
        if defer is True:
            return request
        return request.execute()

    def add_request(self, request, callback=None, request_id=None):
        if (
            len(self.batches) == 0
            or len(self.batches[-1]._requests) >= 100
        ):
            self.batches.append(self.new_batch_http_request())
        try:
            self.batches[-1].add(request, callback, request_id)
        except BatchError:
            self.batches.append(self.new_batch_http_request())
            self.add_request(request, callback, request_id)

    def execute_batches(self, clear_batches=True, raise_errors=True):
        # TODO: add an HTTPError catch, not sure which kind
        execution = tuple(
            map(lambda x: x(), map(attrgetter("execute"), self.batches))
        )
        for batchnum, batch in enumerate(self.batches):
            for reqix, (meta, response) in batch._responses.items():
                response = response.decode('utf-8')
                if len(response) > 0:
                    response = json.loads(response)
                else:
                    response = {}
                error = response.get('error')
                status = meta['status']
                if (error is not None) or (status != '204'):
                    err_rec = {
                        'request': json.loads(
                            self.batches[batchnum]._requests[reqix].to_json()
                        ),
                        'status': status,
                        'response': response,
                        'error': error,
                        'batchnum': batchnum
                    }
                    self.errors.append(err_rec)
        if len(self.errors) > 0 and raise_errors is True:
            raise ExecutionError("some requests failed. check self.errors.")
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


class NoResultsError(ValueError):
    pass


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
        if drivebot.shared_drive_id is not None:
            self.extra_parameters = {
                "includeItemsFromAllDrives": True,
                "supportsAllDrives": True,
                "driveId": drivebot.shared_drive_id,
                "corpora": "drive",
            }
            self.shared_drive_id = drivebot.shared_drive_id
        else:
            self.extra_parameters = {}
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
            **self.extra_parameters,
        }

    def make_manifest(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        if len(self.results) == 0:
            return (
                pd.DataFrame(columns=self.fields),
                pd.DataFrame(columns=self.fields)
            )
        manifest = pd.DataFrame.from_records(self.results)
        manifest = manifest.dropna(subset=["parents"])
        manifest["parents"] = manifest["parents"].str.join("")
        self.directories, self.files = split_on(
            manifest,
            manifest["mimeType"] == "application/vnd.google-apps.folder",
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
        if self.complete is False:
            self.get()
        if len(self.results) == 0:
            return {}, [], root_id
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
