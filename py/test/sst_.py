from options import new_options, Options
from src import Tree
from sst import SSTable
from util import generate_key, generate_random_string
from src.bloom_filter import BloomFilter


def Test():
    options = Options("./data")
    level = 0
    sst_index = 0
    rbtree = Tree()
    for i in range(100):
        rbtree.insert(generate_key(i), generate_random_string())
    # rbtree.show()

    sst = SSTable(options, level, sst_index)
    sst.write(rbtree)


def Test1():
    options = Options("./data")
    level = 0
    sst_index = 0
    rbtree = Tree()
    for i in range(100):
        rbtree.insert(generate_key(i), generate_random_string())
    # rbtree.show()

    sst = SSTable(options, level, sst_index)
    # sst.write(rbtree)
    parse = sst._read_meta()
    print(parse)
    sparse = sst.read_sparse_index()
    print("sparse", sparse)
    for i in sparse:
        records = sst.read_data_by_index(i.block_offset, i.block_length)
        print("records", records)
    print(options.bloom_filter_size, options.bloom_filter_hash_num, options.bloom_filter_size,
          sst.read_bloom_filter())
    bloom_filter = BloomFilter(options.bloom_filter_size, options.bloom_filter_hash_num, options.bloom_filter_seed,
                               sst.read_bloom_filter())

    print("bloom_filter", bloom_filter)
    for i in range(101):
        key = generate_key(i)
        print("key", key, bloom_filter.check(key))

    # key=generate_key(10001)
    # print("key",key,bloom_filter.check(key))
    # records=sst.read_data()
    # print("records",records)


Test()
Test1()
