import argparse
import shutil
import os
from pathlib import Path
import re

from . import read_parquet_metadata
from .utils import pretty_repr



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="reads given parquet's FileMetaData and displays it in an interactive TUI with collapsible sections")

    parser.add_argument(
        "-f",
        "--filepaths",
        nargs="+",
        required=True,
        help="Path(s)/glob-pattern(s) to parquet file(s)"
    )

    parser.add_argument(
        "--expand",
        action="store_true",
        help="[TUI only] expand all collapsible sections at start"
    )

    parser.add_argument(
        "--eager",
        action="store_true",
        help="eagerly load all page headers at start. default is to lazy load on click (or to not load at all incase of --raw)"
    )

    parser.add_argument(
        "--show-None",
        action="store_true",
        help="show `None` attributes as well"
    )

    parser.add_argument(
        "--raw",
        action="store_true",
        help="disable TUI and dump formatted repr directly to stdout"
    )

    args = parser.parse_args()

    paths = []
    for glob_pattern in args.filepaths:
        if not re.search(r'[*?\[\]]', glob_pattern):
            paths.append(Path(glob_pattern))
            continue
        for path in sorted(Path().glob(glob_pattern)):
            paths.append(path)

    metadatas = {}
    total_repr = []

    seen_paths = set()

    for path in paths:
        if path.resolve() in seen_paths:
            continue
        seen_paths.add(path.resolve())
        
        filepath = str(path)

        try:
            metadata = read_parquet_metadata(filepath, lazy_load_pg_hdrs=(not args.eager))
        except Exception as err:
            raise RuntimeError(f"Error while reading metadata of {filepath=}") from err
        
        try:
            repr_ = pretty_repr(metadata, show_None=args.show_None) # reprs[filepath]
        except Exception as err:
            raise RuntimeError(f"Parsing error after reading metadata of {filepath=}") from err

        metadatas[filepath] = metadata

        header = f" {filepath} ".center(shutil.get_terminal_size().columns, "-")
        total_repr.append(f"{header}\n{repr_}")
    
    total_repr = "\n\n".join(total_repr)

    if args.raw:
        print(total_repr)
    else:
        try:
            from .tui import TreeApp
            app = TreeApp(objs=metadatas, expand=args.expand, show_None=args.show_None)
            app.run()
        except Exception:
            print(total_repr, flush=True)
            raise

    