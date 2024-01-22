from pydantic import BaseModel
from typing import List
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

    neurodata_types: List[NeurodataType] = []
    for a in all_assets:
        groups = a.asset.nwb_metadata.groups
        for g in groups:
            if 'neurodata_type' in g.attrs:
                nt = g.attrs['neurodata_type']
                existing = next((n for n in neurodata_types if n.neurodata_type == nt), None)
                if not existing:
                    existing = NeurodataType(neurodata_type=nt, dandiset_ids=[])
                    neurodata_types.append(existing)
                if a.dandiset_id not in existing.dandiset_ids:
                    existing.dandiset_ids.append(a.dandiset_id)
    # sort by neurodata_type
    neurodata_types.sort(key=lambda n: n.neurodata_type)
    for n in neurodata_types:
        print(f'{n.neurodata_type}: {n.dandiset_ids}')

    # Create a markdown table with links to dandisets
    table = []
    for n in neurodata_types:
        dandiset_links = []
        for dandiset_id in n.dandiset_ids:
            dandiset_links.append(f'[{dandiset_id}](https://dandiarchive.org/dandiset/{dandiset_id})')
        table.append([n.neurodata_type, ' '.join(dandiset_links)])

    import datetime

    # Write to neurodata_types.md
    with open('neurodata_types.md', 'w') as f:
        f.write('# Neurodata Types in DANDI Archive\n\n')
        f.write('This is not an exhaustive list. It reflects only a subset of the data that have been parsed to date and favors dandisets that have been updated more recently. Only public dandisets are included.\n\n')
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write(f'Last generated: {timestamp}\n\n')
        f.write(tabulate(table, headers=['Neurodata Type', 'Dandisets'], tablefmt='github'))


if __name__ == '__main__':
    main()
