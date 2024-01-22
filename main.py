from dandi_nwb_meta import process_dandisets


def main():
    process_dandisets(
        max_time=60 * 20,
        max_time_per_dandiset=30
    )


if __name__ == '__main__':
    main()
