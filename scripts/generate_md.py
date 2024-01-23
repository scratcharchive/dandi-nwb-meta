from pydantic import BaseModel
from typing import List, Dict
from dandi_nwb_meta import fetch_all_dandisets, load_existing_output_from_bucket, DandiNwbMetaAsset
from tabulate import tabulate


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
        groups = a.asset.nwb_metadata.groups
        for g in groups:
            if 'neurodata_type' in g.attrs:
                nt = g.attrs.get('namespace', '') + '.' + g.attrs['neurodata_type']
                existing = next((n for n in neurodata_types if n.neurodata_type == nt), None)
                if not existing:
                    existing = NeurodataType(neurodata_type=nt, dandiset_ids=[], path_counts={})
                    neurodata_types.append(existing)
                if a.dandiset_id not in existing.dandiset_ids:
                    existing.dandiset_ids.append(a.dandiset_id)
                if g.path not in existing.path_counts:
                    existing.path_counts[g.path] = 0
                existing.path_counts[g.path] += 1

    # sort by neurodata_type
    neurodata_types = sorted(neurodata_types, key=lambda x: x.neurodata_type)

    # Create a markdown table with links to dandisets
    table1 = []
    for n in neurodata_types:
        dandiset_links = []
        for dandiset_id in sorted(n.dandiset_ids):
            dandiset_links.append(f'[{dandiset_id}](https://dandiarchive.org/dandiset/{dandiset_id})')
        table1.append([n.neurodata_type, ' '.join(dandiset_links)])

    # Create a markdown table with paths
    table2 = []
    for n in neurodata_types:
        paths = n.path_counts.keys()
        paths = sorted(paths, key=lambda x: n.path_counts[x], reverse=True)
        table2.append([n.neurodata_type, ', '.join(_abbrievate([
            f'[{p} ({n.path_counts[p]})]'
            for p in paths
        ], 10))])

    import datetime

    # Write to neurodata_types.md
    with open('neurodata_types.md', 'w') as f:
        f.write('# Neurodata Types in DANDI Archive\n\n')
        f.write('This is not an exhaustive list. It reflects only a subset of the data that have been parsed to date and favors dandisets that have been updated more recently. Only public dandisets are included.\n\n')
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write(f'Last generated: {timestamp}\n\n')
        f.write(tabulate(table1, headers=['Neurodata Type', 'Dandisets'], tablefmt='github'))
        f.write('\n\n')
        f.write(tabulate(table2, headers=['Neurodata Type', 'Paths'], tablefmt='github'))


def _abbrievate(x: List[str], max_num: int):
    if len(x) <= max_num:
        return x
    return x[:max_num] + [f'... and {len(x) - max_num} more...']


if __name__ == '__main__':
    main()
