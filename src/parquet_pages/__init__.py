from typing import Union, Optional, Callable, IO, List
from typing import Generic, TypeVar
import struct
import os
import io
import warnings

import thrift.protocol.TCompactProtocol
from .parquet import ttypes

T = TypeVar("T")

__all__ = ["read_parquet_metadata", "LazyLoaded", "ttypes"]

def read_struct(cls, file, offset: Optional[int] = None):
    obj = cls()
    if offset is not None:
        file.seek(offset, 0)
    obj.read(
        thrift.protocol.TCompactProtocol.TCompactProtocol(
            thrift.transport.TTransport.TFileObjectTransport(
                file
            )
        )
    )
    return obj


class LazyLoaded(Generic[T]):
    def __init__(
        self, 
        file_name: str, 
        col_chunk: ttypes.ColumnChunk, 
        callback: Callable[[IO, ttypes.ColumnChunk], T],
    ):
        self.file_name = file_name
        self.col_chunk = col_chunk
        self.callback = callback
    
    def load(self) -> T:
        with open(self.file_name, "rb") as file:
            return self.callback(file, self.col_chunk)
    
    def __repr__(self) -> str:
        return f"LazyLoaded<..>"


def _read_impl(file, file_len, lazy_load_pg_hdrs) -> ttypes.FileMetaData:
    uint32_parser = struct.Struct("<I")
    assert file.read(4) == b"PAR1"
    
    file.seek(file_len-4)
    assert file.read(4) == b"PAR1"
    
    file.seek(file_len-8)
    metadata_len = uint32_parser.unpack(file.read(4))[0]

        
    metadata = read_struct(
        ttypes.FileMetaData,
        file,
        file_len - 8 - metadata_len,
    )

    file.seek(4, 0)

    for rg in metadata.row_groups:
        for col_chunk in rg.columns:
            if col_chunk.meta_data is None:
                if col_chunk.file_offset == 0:
                    raise RuntimeError(f"no metadata for ColumnChunk")
                warnings.warn("relying on ColumnChunk.file_offset for ColumnChunk.ColumnMetaData is Deprecated", DeprecationWarning)
                col_chunk.meta_data = read_struct(
                    ttypes.ColumnMetaData,
                    file,
                    col_chunk.file_offset,
                )
            
            def read_page_headers(file, col_chunk) -> List[ttypes.PageHeader]:
                first_page_offset = min({
                    col_chunk.meta_data.data_page_offset,
                    col_chunk.meta_data.index_page_offset,
                    col_chunk.meta_data.dictionary_page_offset,
                } - {None,})
                page_headers = []
                file.seek(first_page_offset, 0)
                end_pos = file.tell() + col_chunk.meta_data.total_compressed_size
                while file.tell() < end_pos:
                    page_header = read_struct(ttypes.PageHeader, file)
                    page_headers.append(page_header)
                    file.seek(page_header.compressed_page_size, 1) # skim past actual data
                assert file.tell() == end_pos
                return page_headers
            
            if lazy_load_pg_hdrs:
                file_name = file.name
                assert isinstance(file_name, str) # no sense to lazy load for IO[bytes] objects anyway
                page_headers = LazyLoaded[List[ttypes.PageHeader]](
                    file_name=file.name, 
                    col_chunk=col_chunk, 
                    callback=read_page_headers
                )
            else:
                page_headers = read_page_headers(file, col_chunk)
            
            if col_chunk.offset_index_offset is not None:
                offset_index = read_struct(
                    ttypes.OffsetIndex,
                    file,
                    col_chunk.offset_index_offset,
                )
            else:
                offset_index = None

            if col_chunk.column_index_offset is not None:
                column_index = read_struct(
                    ttypes.ColumnIndex,
                    file,
                    col_chunk.column_index_offset,
                )
            else:
                column_index = None

            col_chunk.page_headers = page_headers
            col_chunk.offset_index = offset_index
            col_chunk.column_index = column_index

    return metadata
                
def _read_from_file(filepath="example.parquet", lazy_load_pg_hdrs: bool = False) -> ttypes.FileMetaData:
    file_len = os.path.getsize(filepath)

    with open(filepath, 'rb') as file:
        return _read_impl(file, file_len, lazy_load_pg_hdrs)

def _read_from_bytes(bytes_obj) -> ttypes.FileMetaData:
    return _read_impl(io.BytesIO(bytes_obj), len(bytes_obj), lazy_load_pg_hdrs=False)


def read_parquet_metadata(source: Union[str, bytes], lazy_load_pg_hdrs: bool = False) -> ttypes.FileMetaData:
    if isinstance(source, str):
        return _read_from_file(filepath=source, lazy_load_pg_hdrs=lazy_load_pg_hdrs)
    elif isinstance(source, bytes):
        return _read_from_bytes(bytes_obj=source)
    else:
        raise NotImplementedError(type(source))
