from pydantic import BaseModel
from typing import List, Dict
from dandi_nwb_meta import fetch_all_dandisets, load_existing_output_from_bucket, DandiNwbMetaAsset
from tabulate import tabulate
import datetime
from h5tojson import H5ToJsonGroup, H5ToJsonDataset, H5ToJsonFile


def main():
    dandisets = fetch_all_dandisets()

    class Asset(BaseModel):
        asset: DandiNwbMetaAsset
        dandiset_id: str

    all_assets: List[Asset] = []

    for dandiset in dandisets:
        X = load_existing_output_from_bucket(dandiset.dandiset_id)
        if not X:
            print(f'No output for {dandiset.dandiset_id}')
            continue
        print(f'Found output for {dandiset.dandiset_id}')
        for a in X.nwb_assets:
            all_assets.append(Asset(asset=a, dandiset_id=dandiset.dandiset_id))

    class NeurodataType(BaseModel):
        neurodata_type: str
        dandiset_ids: List[str]
        path_counts: Dict[str, int]

    neurodata_types: List[NeurodataType] = []
    for a in all_assets:
        all_groups, _ = _get_all_groups_and_datasets(a.asset.nwb_metadata)
        for path, g in all_groups.items():
            if 'neurodata_type' in g.attributes:
                nt = g.attributes.get('namespace', '') + '.' + g.attributes['neurodata_type']
                existing = next((n for n in neurodata_types if n.neurodata_type == nt), None)
                if not existing:
                    existing = NeurodataType(neurodata_type=nt, dandiset_ids=[], path_counts={})
                    neurodata_types.append(existing)
                if a.dandiset_id not in existing.dandiset_ids:
                    existing.dandiset_ids.append(a.dandiset_id)
                if path not in existing.path_counts:
                    existing.path_counts[path] = 0
                existing.path_counts[path] += 1

    # sort by neurodata_type
    neurodata_types = sorted(neurodata_types, key=lambda x: x.neurodata_type)

    # Create a markdown table with links to dandisets
    table1 = []
    for n in neurodata_types:
        dandiset_links = []
        for dandiset_id in sorted(n.dandiset_ids):
            dandiset_links.append(f'[{dandiset_id}](https://dandiarchive.org/dandiset/{dandiset_id})')
        table1.append([n.neurodata_type, ' '.join(dandiset_links)])

    # Write to neurodata_types.md
    with open('neurodata_types_new.md', 'w') as f:
        f.write('# Neurodata Types in DANDI Archive\n\n')
        f.write('This is not an exhaustive list. It reflects only a subset of the data that have been parsed to date and favors dandisets that have been updated more recently. Only public dandisets are included.\n\n')
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write(f'Last generated: {timestamp}\n\n')
        f.write(tabulate(table1, headers=['Neurodata Type', 'Dandisets'], tablefmt='github'))

    # Create a markdown table with paths
    table2 = []
    for n in neurodata_types:
        paths = n.path_counts.keys()
        paths = sorted(paths, key=lambda x: n.path_counts[x], reverse=True)
        table2.append([n.neurodata_type, ', '.join(_abbrievate([
            f'[{p} ({n.path_counts[p]})]'
            for p in paths
        ], 10))])

    # Write to neurodata_types_2.md
    with open('neurodata_types_2_new.md', 'w') as f:
        f.write('# Neurodata Types in DANDI Archive\n\n')
        f.write('This is not an exhaustive list. It reflects only a subset of the data that have been parsed to date and favors dandisets that have been updated more recently. Only public dandisets are included.\n\n')
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write('\n\n')
        f.write(tabulate(table2, headers=['Neurodata Type', 'Paths'], tablefmt='github'))

    # Create a markdown table with neurodata_types for dandisets
    class DandisetInfo(BaseModel):
        dandiset_id: str
        neurodata_types: List[str]
        num_assets_processed: int
    dandiset_infos: List[DandisetInfo] = []
    for a in all_assets:
        existing = next((d for d in dandiset_infos if d.dandiset_id == a.dandiset_id), None)
        if not existing:
            existing = DandisetInfo(dandiset_id=a.dandiset_id, neurodata_types=[], num_assets_processed=0)
            dandiset_infos.append(existing)
        existing.num_assets_processed += 1
        for g in a.asset.nwb_metadata.groups:
            if 'neurodata_type' in g.attrs:
                nt = g.attrs.get('namespace', '') + '.' + g.attrs['neurodata_type']
                if nt not in existing.neurodata_types:
                    existing.neurodata_types.append(nt)
    dandiset_infos = sorted(dandiset_infos, key=lambda x: x.dandiset_id)
    table3 = []
    for d in dandiset_infos:
        dandiset_link = f'[{d.dandiset_id}](https://dandiarchive.org/dandiset/{d.dandiset_id})'
        table3.append([dandiset_link, f'{d.num_assets_processed}', ', '.join(sorted(d.neurodata_types))])
    with open('dandisets_new.md', 'w') as f:
        f.write('# Neurodata Types in DANDI Archive\n\n')
        f.write('This is not an exhaustive list. It reflects only a subset of the data that have been parsed to date and favors dandisets that have been updated more recently. Only public dandisets are included.\n\n')
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write(f'Last generated: {timestamp}\n\n')
        f.write(tabulate(table3, headers=['Dandiset', 'Assets Processed', 'Neurodata Types'], tablefmt='github'))


def _abbrievate(x: List[str], max_num: int):
    if len(x) <= max_num:
        return x
    return x[:max_num] + [f'... and {len(x) - max_num} more...']


def _get_all_groups_and_datasets(f: H5ToJsonFile):
    groups: Dict[str, H5ToJsonGroup] = []
    datasets: Dict[str, H5ToJsonDataset] = []

    def _process_group(g: H5ToJsonGroup, path: str):
        nonlocal groups
        groups[path] = g
        for k, v in g.groups.items():
            _process_group(v, path + '/' + k)
        for k, v in g.datasets.items():
            datasets[path + '/' + k] = v

    _process_group(f.file, '/')
    return groups, datasets


if __name__ == '__main__':
    main()
