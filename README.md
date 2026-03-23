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
or invoke as as a module which dumps repr(metadata) to stdout, with readable indentation
```
 % python -m parquet_pages -h                       
usage: python -m parquet_pages [-h] -f FILEPATH [--default_repr_classes DEFAULT_REPR_CLASSES [DEFAULT_REPR_CLASSES ...]]

dump parquet's FileMetaData pretty-printed to stdout

options:
  -h, --help            show this help message and exit
  -f, --filepath FILEPATH
                        Path to the file
  --default_repr_classes DEFAULT_REPR_CLASSES [DEFAULT_REPR_CLASSES ...]
                        List of class names for which to force reverting to their default __repr__ implementations (default: empty list)
```

To use the above pretty-print inside a script you can:
```python
import parquet_pages.utils
print(parquet_pages.utils.pretty_repr(metadata))
parquet_pages.utils.DEFAULT_REPR_CLASSES = ['SchemaElement', 'Statistics']
print(parquet_pages.utils.pretty_repr(metadata))
```


## Requirements
- Requires [`thrift`](https://pypi.org/project/thrift/) at runtime
- Requires `thrift-compiler` at build time
  - does **NOT** require this if installing from pypi
  - see `Dockerfile`, `Makefile` for build (`make publish`)


## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Third-Party Code

This project includes the file `parquet.thrift` from Apache Parquet Format, developed by the Apache Software Foundation.

That file is licensed under the Apache License, Version 2.0. See `LICENSE-APACHE` and `NOTICE` for details.
