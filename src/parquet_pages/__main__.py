import argparse
from traceback import print_exc
import shutil

from . import read_parquet_metadata
from .utils import pretty_repr



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="reads given parquet's FileMetaData and displays it in an interactive TUI with collapsible sections")

    parser.add_argument(
        "-f",
        "--filepath",
        required=True,
        help="Path to the parquet file"
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

    metadata = read_parquet_metadata(args.filepath, lazy_load_pg_hdrs=(not args.eager))

    repr_ = pretty_repr(metadata, show_None=args.show_None)

    if args.raw:
        print(repr_)
    else:
        try:
            from .tui import TreeApp
            app = TreeApp(obj=metadata, expand=args.expand, show_None=args.show_None)
            app.run()
        except Exception:
            print("".join(["-"]*(shutil.get_terminal_size().columns)))
            print(repr_)
            print("".join(["-"]*(shutil.get_terminal_size().columns)))
            print_exc()

    