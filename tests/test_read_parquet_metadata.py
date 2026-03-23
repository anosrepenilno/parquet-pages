import io
import os

from parquet_pages import read_parquet_metadata, ttypes
import parquet_pages.utils

import polars as pl
import pandas as pd

TMP_ROOT = "tests/tmp"
os.makedirs(TMP_ROOT, exist_ok=True)

def test_1():
    
    b = io.BytesIO()
    df = pl.select(a=pl.arange(0,10), b=pl.arange(20,30).cast(pl.Float32), c=pl.arange(50,100).reshape((10,5)).arr.to_list())
    df.write_parquet(b, compression="zstd")
    b = b.getvalue()

    with open(f"{TMP_ROOT}/example.parquet", 'wb') as f:
        f.write(b)

    metadata = read_parquet_metadata(f"{TMP_ROOT}/example.parquet")
    assert metadata  ==  read_parquet_metadata(b)

    codec = metadata.row_groups[0].columns[0].meta_data.codec 
    assert codec == ttypes.CompressionCodec.ZSTD
    assert codec.__class__ == ttypes.CompressionCodec

    pd.DataFrame({
        'x': [1],
        'y': [b],
    }).set_index('x').to_parquet(f"{TMP_ROOT}/example2.parquet", compression=None)
    metadata2 = read_parquet_metadata(f"{TMP_ROOT}/example2.parquet")
    assert (
        metadata2.row_groups[0].columns[0].meta_data.total_compressed_size == 
        metadata2.row_groups[0].columns[0].meta_data.total_uncompressed_size
    )

    parquet_pages.utils.pretty_repr(metadata)
    parquet_pages.utils.pretty_repr(metadata2)
    parquet_pages.utils.DEFAULT_REPR_CLASSES = ["Statistics", "SchemaElement"]
    parquet_pages.utils.pretty_repr(metadata)
    parquet_pages.utils.pretty_repr(metadata2)

