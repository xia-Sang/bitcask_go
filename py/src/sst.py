import math
import os
from bloom_filter import BloomFilter
from mem_table import Tree
from meta import Meta
from options import Options, bloom_filter
from record import Record, from_bytes_multiple
from sparse_index import SparseIndex, parse_multiple_indices
from util import get_sst_file_path


class SSTable:
    def __init__(self, options: Options, level, sst_index, flag=False):
        self.options = options
        self.file_name = os.path.join(options.dir_path, get_sst_file_path(level, sst_index))
        if flag:
            self._read_meta()

    def write(self, mem_table: Tree, ):
        items = list(mem_table.items())
        chunk_size = len(items) // self.options.table_num
        offset = 0
        sparse_index = []
        bf = bloom_filter(self.options)
        bf.reset()
        with open(self.file_name, "wb") as file:
            for i in range(self.options.table_num):
                buf = bytearray()
                if i == self.options.table_num - 1:
                    chunk = items[i * chunk_size:]
                else:
                    chunk = items[i * chunk_size:(i + 1) * chunk_size]
                cur_length = 0
                for key, value in chunk:
                    bf.add(key)
                    record = Record(key, value)
                    data = record.to_bytes()
                    buf.extend(data)
                    cur_length += len(data)
                min_key = chunk[0][0]
                max_key = chunk[-1][0]
                sparse_index.append(SparseIndex(min_key, max_key, i, offset, cur_length, self.file_name))
                offset += cur_length
                file.write(buf)

            # 写入稀疏索引
            sparse_length = 0
            for sp in sparse_index:
                data = sp.to_bytes()
                sparse_length += len(data)
                file.write(data)

            data = bf.to_bytes()
            file.write(data)

            meta = Meta(offset, sparse_length, len(data))
            file.write(meta.to_bytes())
            file.flush()

    # 删除sst文件
    def delete(self):
        if os.path.exists(self.file_name):
            os.remove(self.file_name)

    # 读取meta信息
    def _read_meta(self):
        with open(self.file_name, "rb") as file:
            file.seek(-12, os.SEEK_END)
            meta_bytes = file.read(12)
            meta = Meta(0, 0, 0)
            meta.from_bytes(meta_bytes)
            self.meta = meta

    def read_data(self):

        with open(self.file_name, "rb") as file:
            file.seek(0, os.SEEK_SET)
            data = file.read(self.meta.block_length)
        records = from_bytes_multiple(data)
        return records

    def read_data_by_index(self, offset, length):

        with open(self.file_name, "rb") as file:
            file.seek(offset, os.SEEK_SET)
            data = file.read(length)
        records = from_bytes_multiple(data)
        return records

    def read_sparse_index(self) -> [SparseIndex]:
        if self.meta == None:
            self.read_data()
        with open(self.file_name, "rb") as file:
            file.seek(self.meta.block_length, os.SEEK_SET)
            sparse_index_bytes = file.read(self.meta.parse_index_length)
        sparse_index = parse_multiple_indices(sparse_index_bytes)
        return sparse_index

    def read_bloom_filter(self):

        with open(self.file_name, "rb") as file:
            file.seek(self.meta.block_length + self.meta.parse_index_length, os.SEEK_SET)
            bloom_filter_bytes = file.read(self.meta.filter_length)
        return bloom_filter_bytes
