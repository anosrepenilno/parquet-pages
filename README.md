# Parquet-Pages
reads a parquet files' metadata information, including each individual data-page headers.
Exposes them directly as thrift structs deserialised in accordance with apache's [parquet.thrift](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift). That file has extensive comments describing the precise meaning of each field (and the lack thereof, if it is deprecated)
These structs include:
- `FileMetaData`
  - `RowGroup` (s)
    - `ColumnChunk` (s)
      - `ColumnMetaData`
      - `OffsetIndex` (if present)
      - `ColumnIndex` (if present)
      - `PageHeader` (s)

(.. etc)

The intended way is to invoke this package as a module to get an interactive TUI that displays this information with collapsible sections. But, one may also instead read it into a `FileMetaData` object inside a script (see details below), in which case the above thrift-generated classes are also exposed via:
```python
from parquet_pages import ttypes
ttypes.FileMetaData
ttypes.ColumnMetaData # etc
```
### NOTE
From a thrift spec perspective, `OffsetIndex` and `ColumnIndex` (when present) are not actually a part of `ColumnChunk`, but are located separately before the rest of the metadata footer. `PageHeader` is not in the footer at all (more details below). But they are presented in the above hierarchy for ease.

## Usage Examples

```
% python -m parquet_pages -h                                    
usage: python -m parquet_pages [-h] -f FILEPATHS [FILEPATHS ...] [--expand] [--eager] [--show-None] [--raw] [--template TEMPLATE]

reads given parquet's FileMetaData and displays it in an interactive TUI with collapsible sections

options:
  -h, --help            show this help message and exit
  -f, --filepaths FILEPATHS [FILEPATHS ...]
                        Path(s)/glob-pattern(s) to parquet file(s)
  --expand              [TUI only] expand all collapsible sections at start
  --eager               eagerly load all page headers at start. default is to lazy load on click (or to not load at all incase of --raw)
  --show-None           show `None` attributes as well
  --raw                 disable TUI and dump formatted repr directly to stdout
  --template TEMPLATE   jinja2 template to filter metadata info
```

- optionally, can also instead dump repr(metadata) to stdout without any TUI, with readable indentation (`--raw`)
- by default, we lazy-load the `PageHeader`s (only read them from the file when requested).
  - this can be changed with the `--eager` flag
  - This is lazy-loaded because all the other thrift-serialised-structs (except `PageHeader`s) are together in the file's footer, but the page-headers (which are before every page's payload) would be scattered all over the file. Eagerly loading them all might end up doing significant I/O if the parquet file is large enough.
    - but since we are only reading the headers and not the payload as well, it wouldn't be more work than reading the entire file contents, for perspective. In most cases eager loading shouldn't feel any slower at all.

![TUI Example](https://raw.githubusercontent.com/anosrepenilno/parquet-pages/main/images/tui_example.png)

- example jinja2 templates:
  - `--template "metadata.row_groups | map(attribute='total_compressed_size') | list"`
    - size of each row-group in the file
  - `--template "metadata.row_groups | map(attribute='columns') | sum(start=[]) | map(attribute='page_headers')  | sum(start=[]) | map(attribute='compressed_page_size') | list"`
    - size of each data/dict/index pages in the file
  - `--template "metadata.row_groups | map(attribute='columns') | map('map', attribute='page_headers') | map('map', 'map', attribute='compressed_page_size') | map('map', 'list') | map('list') | list"`
    - same as last but without flattening hierarchy of row-groups, columns

To read in a script:

```python
from parquet_pages import read_parquet_metadata

metadata = read_parquet_metadata("example.parquet")

with open("example2.parquet", "rb") as file:
    file_contents = file.read()

metadata2 = read_parquet_metadata(file_contents)
```

- This eagerly-loads `PageHeader`s by default, for simplicity.
- To load them lazily like what happens in the TUI, we can pass the argument `lazy_load_pg_hdrs=True`
  - the argument is ignored when reading from a `bytes` object, as it only would have made a difference when reading from a file on disk.
  - it replaces `PageHeader` objects with `parquet_bytes.LazyLoaded` objects, calling whose `.load()` method will actually read and return the struct.


## Requirements
### Runtime
- Requires [`thrift`](https://pypi.org/project/thrift/)
- Requires [`jinja2`](https://pypi.org/project/jinja2/)
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
