import os

from bloom_filter import BloomFilter


class Options:
    def __init__(self, dir_path, mem_table_size=1024, max_sst_size=1024, max_level=7, max_level_num=7, table_num=10):
        self.bloom_filter_size = 1024
        self.bloom_filter_hash_num = 3
        self.bloom_filter_seed = 1

        self.dir_path = dir_path
        self.max_sst_size = max_sst_size
        self.max_level = max_level
        self.max_level_num = max_level_num
        self.table_num = table_num
        self.mem_table_size = mem_table_size

        self._set_default_options()

    def __str__(self) -> str:
        return f"Options(dir_path={self.dir_path}, max_sst_size={self.max_sst_size}, max_level={self.max_level}, max_level_num={self.max_level_num}, table_num={self.table_num})"

    def _set_default_options(self):
        if self.max_sst_size <= 0:
            self.max_sst_size = 1024
        if self.max_level <= 0:
            self.max_level = 7
        if self.max_level_num <= 0:
            self.max_level_num = 7
        if self.table_num <= 0:
            self.table_num = 10
        if self.mem_table_size <= 0:
            self.mem_table_size = 1024

    def check(self):
        try:
            os.makedirs(self.dir_path, exist_ok=True)
            os.makedirs(os.path.join(self.dir_path, "wal"), exist_ok=True)
        except OSError as e:
            return e
        return None


def bloom_filter_with_data(opts: Options, data):
    return BloomFilter(opts.bloom_filter_size, opts.bloom_filter_hash_num, opts.bloom_filter_seed, data)


def bloom_filter(opts: Options):
    return BloomFilter(opts.bloom_filter_size, opts.bloom_filter_hash_num, opts.bloom_filter_seed)


# 配置函数
def with_max_sst_size(size):
    def option(o):
        o.max_sst_size = size

    return option


def with_max_level(level):
    def option(o):
        o.max_level = level

    return option


def with_max_level_num(num):
    def option(o):
        o.max_level_num = num

    return option


def with_table_num(num):
    def option(o):
        o.table_num = num

    return option


# 工厂函数
def new_options(dir_path, *opts):
    opt = Options(dir_path)

    for op in opts:
        op(opts)

    # error = opts.check()
    # if error:
    #     return None, error

    return opts, None

# # 使用示例
# options, err = new_options("/path/to/config",
#                            with_max_sst_size(2048),
#                            with_max_level(10),
#                            with_max_level_num(5),
#                            with_table_num(20))
#
# if err:
#     print(f"Error: {err}")
# else:
#     print(f"Options: {options.__dict__}")
