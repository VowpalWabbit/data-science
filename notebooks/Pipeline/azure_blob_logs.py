from azure.storage.blob import ContainerClient, BlobServiceClient
from Pipeline.progress import dummy_progress
import datetime
import os
import pandas as pd
import multiprocessing

def _load_last_modified(local_path):
    meta_path = f'{local_path}.lm'
    if not os.path.exists(meta_path):
        return None
    meta = pd.to_datetime(open(meta_path, 'r').read())
    return meta if isinstance(meta, datetime.datetime) else None

def _save_last_modified(last_modified, local_path):
    with open(f'{local_path}.lm','w') as f:
        f.write(str(last_modified))

def _add_path(subtree, parts, last_modified):
    if len(parts) == 1:
        subtree[parts[0]] = last_modified
    else:
        if parts[0] not in subtree:
            subtree[parts[0]] = {}
        _add_path(subtree[parts[0]], parts[1:], last_modified)


def _get_file_tree(blobs_with_last_modified):
    result = {}
    for b, last_modified in blobs_with_last_modified:
        _add_path(result, b.split('/'), last_modified)
    return result

def _goto(tree, path:list=[]):
    cur = tree
    for p in path:
        if p not in cur:
            raise Exception(f'Cannot find {p} from {path}')
        cur = cur[p]
    return cur

def _get_folders(tree, path:list=[], prefix='', full_path=False):
    cur = _goto(tree, path)
    return [k if not full_path else '/'.join(path+[k]) for k in cur if not isinstance(cur[k], datetime.datetime) and k.startswith(prefix)]   

def _get_files(tree, path:list=[], prefix='', full_path=False, recursive=False):
    cur = _goto(tree, path)
    result = [k if not full_path else '/'.join(path+[k]) for k in cur if isinstance(cur[k], datetime.datetime) and k.startswith(prefix)]
    if recursive:
        for f in _get_folders(tree, path, full_path):
            result = result + _get_files(tree, path + f, full_path)  
    return result

def _to_local_path(path):
    return os.path.join(*path.split('/'))

class Container:
    def __init__(self, container_client, local_folder, prefix='', progress = dummy_progress()):
        self._impl = container_client
        self.refresh(prefix)
        self.local_folder = os.path.join(local_folder, container_client.container_name)
        self.progress = progress

    def refresh(self, prefix=''):
        self._tree = _get_file_tree([(b['name'], b['last_modified']) for b in self._impl.list_blobs(name_starts_with=prefix)])

    def _download(self, path, local_path, max_concurrency=multiprocessing.cpu_count()):
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        self._impl.download_blob(path).download_to_stream(open(local_path, "wb"), max_concurrency=max_concurrency)
        return True

    def _sync(self, path, local_path, last_modified=None, max_concurrency=multiprocessing.cpu_count(), update=True):
        lm_local = _load_last_modified(local_path)
        lm_remote = last_modified if last_modified else self.get_last_modified(path)
        if not lm_local or lm_local < lm_remote:
            if update or not lm_local:
                if self._download(path, local_path, max_concurrency=max_concurrency):
                    _save_last_modified(lm_remote, local_path)
                else:
                    return None
        self.progress.on_step()
        return local_path

    def get_last_modified(self, path):
        for blob in self._impl.list_blobs(name_starts_with=path):
            if blob['name']==path:
                return blob['last_modified']
        return None

    def sync_blobs(self, paths: list, max_concurrency=multiprocessing.cpu_count(), update=True):
        self.progress.on_start(len(paths))
        result = [self._sync(p, os.path.join(self.local_folder, _to_local_path(p)), update=update) for p in paths]
        self.progress.on_finish()
        return result

    def sync_folder(self, folder, max_concurrency=multiprocessing.cpu_count(), update=True):
        if folder[-1] != '/': folder = folder + '/'
        blobs = self._impl.list_blobs(name_starts_with=folder)
        self.progress.on_start(len(blobs))
        result = sorted([self._sync(p['name'], os.path.join(self.local_folder, _to_local_path(p['name'])), p['last_modified'], update=update) 
            for p in blobs])
        self.progress.on_finish()
        return result

class LogsClient(Container):
    def __init__(self, container_client, local_folder, progress = dummy_progress()):
        super().__init__(container_client, local_folder, progress=progress)
        
    def get_models(self):
        import re
        p = re.compile(r'\d+')
        return sorted([f for f in _get_folders(self._tree) if p.match(f)])

    def get_dates(self, model, years: list=None, months: list=None):
        result = set([])
        model_tree = _goto(self._tree,  [model, 'data'])
        ys = years if years else _get_folders(model_tree)
        for y in ys:
            year_tree = model_tree[y]
            ms = months if months else _get_folders(year_tree)
            for m in ms:
                month_tree = year_tree[m]
                for d in _get_files(month_tree):
                    result.add(datetime.date(int(y), int(m), int(d[0:2])))
        return sorted(list(result))

    def get_chunks(self, model, date, full_path=True):
        return _get_files(self._tree,
            [model, 'data', str(date.year), str(date.month).zfill(2)],
            prefix=str(date.day).zfill(2),
            full_path=full_path)

    def get_chunk_ids(self, model, date):
        return sorted([int(p[3:p.find('.')]) for p in _get_files(self._tree,
            [model, 'data', str(date.year), str(date.month).zfill(2)],
            prefix=str(date.day).zfill(2),
            full_path=False)])

    def get_full_path(self, model, date, chunk_id: int, is_local=False):
        path = [model, 'data', str(date.year), str(date.month).zfill(2), f'{str(date.day).zfill(2)}_{str(chunk_id).zfill(10)}.json']
        return '/'.join(path) if not is_local else os.path.join(*path)

def connect(ws, loop_name, local_folder, from_connection_string=False, progress=dummy_progress()):
    from azureml.core import Datastore
    from azure.storage.blob import ContainerClient
    datastore = Datastore.get(ws, loop_name)
    if from_connection_string:
        client = ContainerClient.from_connection_string(datastore.blob_service.authentication.sas_token, container_name=loop_name)
    else:
        client = ContainerClient.from_container_url(datastore.blob_service.authentication.sas_token)
    return LogsClient(client, local_folder, progress=progress)

def get_cloud_logs(loop, local_root, latest_folder_only=True, progress=dummy_progress()):
    from azureml.core import Workspace
    import itertools
    from pathlib import Path
    ws = Workspace.from_config()
    logs = connect(ws, loop, Path(local_root).joinpath('original'), progress=progress)
    models = logs.get_models()
    if latest_folder_only:
        models = models[-1:]
    dates = {model: logs.get_dates(model) for model in models}
    cloud_files = []
    for model in models:
        cloud_files += itertools.chain.from_iterable([logs.get_chunks(model, d) for d in dates[model]])
    return cloud_files

def sync_logs(loop, local_root, latest_folder_only=True, progress=dummy_progress(), days = 31):
    from azureml.core import Workspace
    import itertools
    from pathlib import Path
    ws = Workspace.from_config()
    logs = connect(ws, loop, local_root, progress=progress)
    models = logs.get_models()
    if latest_folder_only:
        models = models[-1:]
    dates = {model: [d for d in logs.get_dates(model) if d > datetime.date.today() - datetime.timedelta(days)] for model in models}
    cloud_files = []
    for model in models:
        cloud_files += itertools.chain.from_iterable([logs.get_chunks(model, d) for d in dates[model]])
    return logs.sync_blobs(cloud_files)


