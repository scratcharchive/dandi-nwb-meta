import os
from tempfile import TemporaryDirectory
import dandi.dandiarchive as da
import json
import warnings
import urllib
import time
from typing import List
from typing import Union, Any
from pydantic import BaseModel, Field
import gzip
import boto3
from h5tojson import h5_to_object, H5ToJsonFile, H5ToJsonOpts


def process_dandisets(
    *,
    max_time: float,
    max_time_per_dandiset: float
):
    dandisets = fetch_all_dandisets()

    timer = time.time()
    for dandiset in dandisets:
        print("")
        print(f"Processing {dandiset.dandiset_id} version {dandiset.version}")
        process_dandiset(dandiset.dandiset_id, max_time_per_dandiset)
        elapsed = time.time() - timer
        print(f'Time elapsed: {elapsed} seconds')
        if elapsed > max_time:
            print("Time limit reached.")
            break


class Dandiset(BaseModel):
    dandiset_id: str
    version: str


def fetch_all_dandisets():
    url = 'https://api.dandiarchive.org/api/dandisets/?page=1&page_size=5000&ordering=-modified&draft=true&empty=false&embargoed=false'
    with urllib.request.urlopen(url) as response:
        X = json.loads(response.read())

    dandisets: List[Dandiset] = []
    for ds in X['results']:
        pv = ds['most_recent_published_version']
        dv = ds['draft_version']
        dandisets.append(Dandiset(
            dandiset_id=ds['identifier'],
            version=pv['version'] if pv else dv['version']
        ))

    return dandisets


def process_dandiset(
    dandiset_id: str,
    max_time: float
):
    timer = time.time()

    if os.environ.get('AWS_ACCESS_KEY_ID') is not None:
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
            endpoint_url=os.environ['S3_ENDPOINT_URL'],
            region_name='auto'  # for cloudflare
        )
    else:
        s3 = None

    # Load existing output
    print('Checking for existing output')
    existing = _load_existing_output(s3, dandiset_id)
    if existing is not None:
        print(f'Found {len(existing.nwb_assets)} existing assets.')
    else:
        print('No existing output found.')

    # Create the dandi parsed url
    parsed_url = da.parse_dandi_url(f"https://dandiarchive.org/dandiset/{dandiset_id}")

    # Create the new output
    X = DandiNwbMetaDandiset(
        dandiset_id=dandiset_id, dandiset_version="draft", nwb_assets=[]
    )

    something_changed = False
    if existing is None:
        something_changed = True
    with parsed_url.navigate() as (client, dandiset, assets):
        asset_num = 0
        # Loop through all assets in the dandiset
        for asset in dandiset.get_assets():
            asset_num += 1
            if asset.path.endswith(".nwb"):  # only process NWB files
                # Check if the asset has already been processed
                item = next(
                    (x for x in existing.nwb_assets if x.asset_id == asset.identifier),
                    None,
                ) if existing else None
                if item:
                    # The asset has already been processed in the output file
                    print(f"{asset_num}: {X.dandiset_id} | {asset.path} | already processed")
                    X.nwb_assets.append(item)
                    continue
                print(f"{asset_num}: {X.dandiset_id} | {asset.path}")
                opts = H5ToJsonOpts(
                    skip_all_dataset_data=True
                )
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    nwb_metadata = h5_to_object(asset.download_url, opts)
                # Create the new asset
                A = DandiNwbMetaAsset(
                    asset_id=asset.identifier,
                    asset_path=asset.path,
                    nwb_metadata=nwb_metadata
                )
                # # Open the file for lazy loading
                # file = remfile.File(asset.download_url, verbose=False)
                # with h5py.File(file, "r") as h5_file:
                #     all_groups_in_h5_file = _get_h5_groups(h5_file)
                #     # Add the groups to the asset
                #     for group in all_groups_in_h5_file:
                #         A.nwb_metadata.groups.append(
                #             H5MetadataGroup(
                #                 path=group.name, attrs=json.loads(_attrs_to_json(group))
                #             )
                #         )
                #     # Add the datasets to the asset
                #     all_datasets_in_h5_file = _get_h5_datasets(h5_file)
                #     for dataset in all_datasets_in_h5_file:
                #         dataset: h5py.Dataset = dataset
                #         dtype = _dtype_to_str(dataset)
                #         A.nwb_metadata.datasets.append(
                #             H5MetadataDataset(
                #                 path=dataset.name,
                #                 attrs=json.loads(_attrs_to_json(dataset)),
                #                 shape=_format_shape(dataset),
                #                 chunks=[d for d in dataset.chunks] if dataset.chunks else None,
                #                 compression=dataset.compression,
                #                 compression_opts=dataset.compression_opts,
                #                 dtype=dtype,
                #             )
                #         )
                # Add the asset to the dandiset
                X.nwb_assets.append(A)
                something_changed = True
            if time.time() - timer > max_time:
                print("Time limit reached for this dandiset.")
                break
    if something_changed:
        print(f'Saving output for {dandiset_id}')
        _save_output(s3, dandiset_id, X)
    else:
        print(f'Not saving output for {dandiset_id} because nothing changed.')


class H5MetadataGroup(BaseModel):
    path: str = Field(description="Path to the group")
    attrs: dict = Field(description="Attributes of the group")


class H5MetadataDataset(BaseModel):
    path: str = Field(description="Path to the dataset")
    attrs: dict = Field(description="Attributes of the dataset")
    shape: list = Field(description="Shape of the dataset")
    dtype: str = Field(description="Data type of the dataset")
    chunks: Union[list, None] = Field(description="Chunk shape of the dataset")
    compression: Union[str, None] = Field(description="Compression type of the dataset")
    compression_opts: Union[Any, None] = Field(description="Compression options of the dataset")


class DandiNWbMetaAssetNwbMetadata(BaseModel):
    groups: List[H5MetadataGroup] = Field(description="HDF5 group metadata")
    datasets: List[H5MetadataDataset] = Field(description="HDF5 dataset metadata")


class DandiNwbMetaAsset(BaseModel):
    asset_id: str = Field(description="Asset identifier")
    asset_path: str = Field(description="Asset path")
    nwb_metadata: H5ToJsonFile = Field(description="NWB metadata")


class DandiNwbMetaDandiset(BaseModel):
    dandiset_id: str = Field(description="Dandiset identifier")
    dandiset_version: str = Field(description="Dandiset version")
    nwb_assets: List[DandiNwbMetaAsset] = Field(description="List of assets")


# def _get_h5_groups(h5_file: h5py.File) -> list:
#     """Returns a list of all groups in an h5 file.

#     Args:
#         h5_file (h5py.File): The h5 file.

#     Returns:
#         list: A list of all groups in the h5 file.
#     """
#     groups = []

#     def _process_node(node: h5py.Group):
#         groups.append(node)
#         for child in node.values():
#             if isinstance(child, h5py.Group):
#                 _process_node(child)
#     _process_node(h5_file)
#     return groups


# def _get_h5_datasets(h5_file: h5py.File) -> list:
#     """Returns a list of all datasets in an h5 file.

#     Args:
#         h5_file (h5py.File): The h5 file.

#     Returns:
#         list: A list of all datasets in the h5 file.
#     """
#     datasets = []

#     def _process_node(node: h5py.Group):
#         for child in node.values():
#             if isinstance(child, h5py.Dataset):
#                 datasets.append(child)
#             elif isinstance(child, h5py.Group):
#                 _process_node(child)
#     _process_node(h5_file)
#     return datasets


# def _attrs_to_json(group: Union[h5py.Group, h5py.Dataset]) -> str:
#     """Converts the attributes of an HDF5 group or dataset to a JSON-serializable format."""
#     attrs_dict = {}
#     for attr_name in group.attrs:
#         value = group.attrs[attr_name]

#         # Convert NumPy arrays to lists
#         if isinstance(value, np.ndarray):
#             value = value.tolist()
#         # Handle other non-serializable types as needed
#         elif isinstance(value, np.int64):
#             value = int(value)
#         # Handle References
#         elif isinstance(value, h5py.Reference):
#             value = str(value)

#         # check if json serializable
#         try:
#             json.dumps(value)
#         except TypeError:
#             value = "Not JSON serializable"

#         attrs_dict[attr_name] = value

#     return json.dumps(attrs_dict)


# def _dtype_to_str(dataset: h5py.Dataset) -> str:
#     """Converts the dtype of an HDF5 dataset to a string."""
#     dtype = dataset.dtype
#     if dtype == np.dtype("int8"):
#         return "int8"
#     elif dtype == np.dtype("uint8"):
#         return "uint8"
#     elif dtype == np.dtype("int16"):
#         return "int16"
#     elif dtype == np.dtype("uint16"):
#         return "uint16"
#     elif dtype == np.dtype("int32"):
#         return "int32"
#     elif dtype == np.dtype("uint32"):
#         return "uint32"
#     elif dtype == np.dtype("int64"):
#         return "int64"
#     elif dtype == np.dtype("uint64"):
#         return "uint64"
#     elif dtype == np.dtype("float32"):
#         return "float32"
#     elif dtype == np.dtype("float64"):
#         return "float64"
#     else:
#         # raise ValueError(f"Unsupported dtype: {dtype}")
#         return "Unsupported dtype"


# def _format_shape(dataset: h5py.Dataset) -> list:
#     """Formats the shape of an HDF5 dataset to a list."""
#     shape = dataset.shape
#     return [int(dim) for dim in shape]


def load_existing_output_from_bucket(dandiset_id: str) -> DandiNwbMetaDandiset:
    with TemporaryDirectory() as tempdir:
        object_key = _get_object_key_for_output(dandiset_id)
        url = f'https://neurosift.org/{object_key}'
        tmp_output_fname = os.path.join(tempdir, 'output.json.gz')
        try:
            _download_file(url, tmp_output_fname)
        except urllib.error.HTTPError:
            return None
        return _load_existing_output_from_file(tmp_output_fname)


def _load_existing_output(s3: Union[Any, None], dandiset_id: str) -> DandiNwbMetaDandiset:
    """Loads the existing output for a dandiset."""
    if s3 is not None:
        return load_existing_output_from_bucket(dandiset_id)
    else:
        output_fname = f"dandisets/{dandiset_id}.json"
        return _load_existing_output_from_file(output_fname)


def _get_object_key_for_output(dandiset_id: str) -> str:
    return f'dandi-nwb-meta-2/dandisets/{dandiset_id}.json.gz'


def _load_existing_output_from_file(output_fname: str) -> DandiNwbMetaDandiset:
    if os.path.exists(output_fname):
        if output_fname.endswith(".gz"):
            with open(output_fname, "rb") as f:
                existing = json.loads(gzip.decompress(f.read()))
                existing = DandiNwbMetaDandiset(**existing)
        else:
            with open(output_fname, "r") as f:
                existing = json.load(f)
                existing = DandiNwbMetaDandiset(**existing)
    else:
        existing = None
    return existing


def _download_file(url: str, output_fname: str):
    """Downloads a file from a URL."""
    with open(output_fname, "wb") as f:
        # The User-Agent header is required so that cloudflare doesn't block the request
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req) as response:
            chunk_size = 1024
            while True:
                chunk = response.read(chunk_size)
                if not chunk:
                    break
                f.write(chunk)


def _save_output(s3: Union[Any, None], dandiset_id: str, X: DandiNwbMetaDandiset):
    """Saves the output for a dandiset."""
    if s3 is not None:
        with TemporaryDirectory() as tempdir:
            tmp_output_fname = os.path.join(tempdir, 'output.json.gz')
            _save_output_to_file(tmp_output_fname, X)
            object_key = _get_object_key_for_output(dandiset_id)
            print(f'Uploading output to {object_key}')
            _upload_file_to_s3(s3, 'neurosift', object_key, tmp_output_fname)
    else:
        output_fname = f"dandisets/{dandiset_id}.json"
        # make the output directory if it doesn't exist
        if not os.path.exists("dandisets"):
            os.makedirs("dandisets")
        _save_output_to_file(output_fname, X)


def _upload_file_to_s3(s3, bucket, object_key, fname):
    if fname.endswith('.html'):
        content_type = 'text/html'
    elif fname.endswith('.js'):
        content_type = 'application/javascript'
    elif fname.endswith('.css'):
        content_type = 'text/css'
    elif fname.endswith('.png'):
        content_type = 'image/png'
    elif fname.endswith('.jpg'):
        content_type = 'image/jpeg'
    elif fname.endswith('.svg'):
        content_type = 'image/svg+xml'
    elif fname.endswith('.json'):
        content_type = 'application/json'
    elif fname.endswith('.gz'):
        content_type = 'application/gzip'
    else:
        content_type = None
    extra_args = {}
    if content_type is not None:
        extra_args['ContentType'] = content_type
    s3.upload_file(fname, bucket, object_key, ExtraArgs=extra_args)


def _save_output_to_file(output_fname: str, X: DandiNwbMetaDandiset):
    if output_fname.endswith(".gz"):
        with gzip.open(output_fname, "wb") as f:
            f.write(json.dumps(_remove_empty_dicts_in_dict(X.dict())).encode())
    else:
        with open(output_fname, "w") as f:
            json.dump(_remove_empty_dicts_in_dict(X.dict()), f, indent=2)


def _remove_empty_dicts_in_dict(x: dict):
    ret = {}
    for k, v in x.items():
        if isinstance(v, dict):
            if not v:
                continue
            v2 = _remove_empty_dicts_in_dict(v)
        elif isinstance(v, list):
            v2 = _remove_empty_dicts_in_list(v)
        else:
            v2 = v
        ret[k] = v2
    return ret


def _remove_empty_dicts_in_list(x: list):
    ret = []
    for v in x:
        if isinstance(v, dict):
            v2 = _remove_empty_dicts_in_dict(v)
        elif isinstance(v, list):
            v2 = _remove_empty_dicts_in_list(v)
        else:
            v2 = v
        ret.append(v2)
    return ret
