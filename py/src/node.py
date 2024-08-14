import os.path

from bloom_filter import BloomFilter
from options import Options, bloom_filter_with_data
from sparse_index import SparseIndex
from sst import SSTable
from util import get_sst_file_path


class Node:
    def __init__(self, options: Options, level, sst_index):
        self.options = options
        self.file_name = os.path.join(options.dir_path, get_sst_file_path(level, sst_index))
        self.sst_reader = SSTable(options, level, sst_index, True)

        self.parse_index: [SparseIndex] = self._read_parse_index()
        self.bloom_filter = self._read_bloom_filter()

    def search(self, key):
        if not self.bloom_filter.check(key):
            return "", False
        for pi in self.parse_index:
            """
                self.min_key=min_key
                self.max_key=max_key
                self.block_index=block_index
                self.block_offset=block_offset
                self.block_length=block_length
                self.file_name=file_name
            """
            if key < pi.min_key or key > pi.max_key:
                continue
            records = self._read_block_data(pi.block_offset, pi.block_length)
            for record in records:
                if record.key == key:
                    return record.value, True
        return "", False

    def _read_bloom_filter(self):
        filter_data = self.sst_reader.read_bloom_filter()
        return bloom_filter_with_data(self.options, filter_data)

    def _read_parse_index(self):
        return self.sst_reader.read_sparse_index()

    def _read_block_data(self, offset, length):
        return self.sst_reader.read_data_by_index(offset, length)

    def read_data(self):
        ls=[]
        for pi in self.parse_index:
            records = self._read_block_data(pi.block_offset, pi.block_length)
            ls.extend(records)
        return ls

    def delete(self):
        self.sst_reader.delete()
