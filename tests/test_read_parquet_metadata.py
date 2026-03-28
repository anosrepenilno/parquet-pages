import io
import os

from parquet_pages import read_parquet_metadata, ttypes, LazyLoaded
from parquet_pages.utils import pretty_repr
from parquet_pages.tui import TreeApp

import polars as pl
import pandas as pd
from textual.widgets import Tree

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

    assert "None" not in pretty_repr(metadata, show_None=False)
    assert "None" not in pretty_repr(metadata2, show_None=False)
    repr_ = pretty_repr(metadata, show_None=True)
    assert "None" in repr_
    assert repr_.count("None") == 148
    repr_ = pretty_repr(metadata2, show_None=True)
    assert "None" in repr_
    assert repr_.count("None") == 78
    print(repr_)

    tree = list(TreeApp({"example.parquet": metadata}).compose())[1]
    assert isinstance(tree, Tree)
    assert len(tree._tree_nodes) == 218
    tree = list(TreeApp({"example.parquet": metadata}, show_None=True).compose())[1]
    assert isinstance(tree, Tree)
    assert len(tree._tree_nodes) == 366

    metadata_lazy = read_parquet_metadata(f"{TMP_ROOT}/example.parquet", lazy_load_pg_hdrs=True)
    for rg_lazy, rg in zip(metadata_lazy.row_groups, metadata.row_groups):
        for col_lazy, col in zip(rg_lazy.columns, rg.columns):
            assert isinstance(col_lazy.page_headers, LazyLoaded)
            col_lazy.page_headers.load() == col.page_headers

    tree = list(TreeApp({"example.parquet": metadata_lazy}).compose())[1]
    assert isinstance(tree, Tree)
    assert len(tree._tree_nodes) == 172
    tree = list(TreeApp({"example.parquet": metadata_lazy}, show_None=True).compose())[1]
    assert isinstance(tree, Tree)
    assert len(tree._tree_nodes) == 288


