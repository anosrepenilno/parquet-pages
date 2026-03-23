from typing import Union, Optional
import struct
import os
import io
import warnings

import thrift.protocol.TCompactProtocol
from .parquet import ttypes


__all__ = ["read_parquet_metadata"]

def _read_impl(file, file_len) -> ttypes.FileMetaData:
    uint32_parser = struct.Struct("<I")
    assert file.read(4) == b"PAR1"
    
    file.seek(file_len-4)
    assert file.read(4) == b"PAR1"
    
    file.seek(file_len-8)
    metadata_len = uint32_parser.unpack(file.read(4))[0]

    def reader(offset: Optional[int] = None):
        if offset is not None:
            file.seek(offset, 0)
        return thrift.protocol.TCompactProtocol.TCompactProtocol(
            thrift.transport.TTransport.TFileObjectTransport(
                file
            )
        )

    def read_struct(cls, offset: Optional[int] = None):
        obj = cls()
        obj.read(reader(offset=offset))
        return obj
        
    metadata = read_struct(
        ttypes.FileMetaData,
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
                    col_chunk.file_offset,
                )
            
            first_page_offset = min({
                col_chunk.meta_data.data_page_offset,
                col_chunk.meta_data.index_page_offset,
                col_chunk.meta_data.dictionary_page_offset,
            } - {None,})
            page_headers = []

            file.seek(first_page_offset, 0)
            end_pos = file.tell() + col_chunk.meta_data.total_compressed_size
            while file.tell() < end_pos:
                page_header = read_struct(ttypes.PageHeader)
                page_headers.append(page_header)
                file.seek(page_header.compressed_page_size, 1) # skim past actual data
            assert file.tell() == end_pos
            
            if col_chunk.offset_index_offset is not None:
                offset_index = read_struct(
                    ttypes.OffsetIndex,
                    col_chunk.offset_index_offset,
                )
            else:
                offset_index = None

            if col_chunk.column_index_offset is not None:
                column_index = read_struct(
                    ttypes.ColumnIndex,
                    col_chunk.column_index_offset,
                )
            else:
                column_index = None

            col_chunk.page_headers = page_headers
            col_chunk.offset_index = offset_index
            col_chunk.column_index = column_index

    return metadata
                
def _read_from_file(filepath="example.parquet") -> ttypes.FileMetaData:
    file_len = os.path.getsize(filepath)

    with open(filepath, 'rb') as file:
        return _read_impl(file, file_len)

def _read_from_bytes(bytes_obj) -> ttypes.FileMetaData:
    return _read_impl(io.BytesIO(bytes_obj), len(bytes_obj))


def read_parquet_metadata(source: Union[str, bytes]) -> ttypes.FileMetaData:
    if isinstance(source, str):
        return _read_from_file(filepath=source)
    elif isinstance(source, bytes):
        return _read_from_bytes(bytes_obj=source)
    else:
        NotImplementedError(type(source))
