import argparse

from . import read_parquet_metadata
from . import utils


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="dump parquet's FileMetaData pretty-printed to stdout")

    parser.add_argument(
        "-f",
        "--filepath",
        required=True,
        help="Path to the file"
    )

    parser.add_argument(
        "--default_repr_classes",
        nargs="+",          # one or more values
        default=[],         # empty list if not provided
        help="List of class names for which to force reverting to their default __repr__ implementations (default: empty list)"
    )

    args = parser.parse_args()

    utils.DEFAULT_REPR_CLASSES=args.default_repr_classes

    metadata = read_parquet_metadata(args.filepath)
    print(utils.pretty_repr(metadata))