# Parquet-Pages
read a parquet files' metadata information, including each individual data-page headers.
Exposes them directly as thrift structs deserialised in accordance with parquet.thrift (included from [apache/parquet-format](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift))
These structs include:
- `FileMetaData`
  - `RowGroup` (s)
    - `ColumnChunk` (s)
      - `ColumnMetaData`
      - `OffsetIndex`
      - `ColumnIndex`
      - `PageHeader` (s)

can refer to these directly as:
```python
from parquet_pages import ttypes
ttypes.FileMetaData # etc
```

Everything except the `PageHeader` structs are in the file's footer. So if you don't need the `PageHeader`s, there are other simpler tools to get the rest.
Since this tool reads the `PageHeader`s too (which are scattered all over the file) it might end up doing significant I/O if the parquet file is large enough.
So it is really only intended for development-time inspection/debugging.

## Usage Examples
```python
from parquet_pages import read_parquet_metadata

metadata = read_parquet_metadata("example.parquet")

with open("example2.parquet", "rb") as file:
    file_contents = file.read()

metadata2 = read_parquet_metadata(file_contents)
```
or invoke as as a module which displays it in an interactive TUI with collapsible sections.

optionally, can also instead dump repr(metadata) to stdout without any TUI, with readable indentation (`--raw`)
```
% python -m parquet_pages --help                              
usage: python -m parquet_pages [-h] -f FILEPATH [--expand] [--raw]

reads given parquet's FileMetaData and displays it in an interactive TUI with collapsible sections

options:
  -h, --help            show this help message and exit
  -f, --filepath FILEPATH
                        Path to the parquet file
  --expand              [TUI only] expand all collapsible sections at start
  --raw                 disable TUI and dump formatted repr directly to stdout
```

![TUI Example](https://raw.githubusercontent.com/anosrepenilno/parquet-pages/main/images/tui_example.png)

## Requirements
### Runtime
- Requires [`thrift`](https://pypi.org/project/thrift/)
- Only optionally requires, but recommended: [`textual`](https://pypi.org/project/textual/)
  - not optional during install, so would be installed automatically, but someone might uninstall it and core functionality will still work
    - `parquet_pages.read_parquet_metadata` does not require it
    - `python -m parquet_pages --raw ...` does not require it
  - only to manage TUI display after metadata is read
### Build-time
- Requires `thrift-compiler`
  - but does **NOT** require this if installing from pypi
  - see `Dockerfile`, `Makefile` (`make publish`)


## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Third-Party Code

This project includes the file `parquet.thrift` from Apache Parquet Format, developed by the Apache Software Foundation.

That file is licensed under the Apache License, Version 2.0. See `LICENSE-APACHE` and `NOTICE` for details.
