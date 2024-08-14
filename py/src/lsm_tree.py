import os
from mem_table import Tree
from node import Node
from options import Options, new_options
from sst import SSTable
from wal import WriteAheadLog
from record import Record
from util import *
from bloom_filter import BloomFilter
import asyncio


class OldMemTable:
    def __init__(self, mem_table_index, mem_table):
        self.mem_table_index = mem_table_index
        self.mem_table = mem_table

    def __str__(self):
        return f"mem_table_index: {self.mem_table_index}, mem_table: {self.mem_table}"


class LSMTree:
    def __init__(self, opts: Options):
        self.options = opts
        self.mem_table = Tree()
        self.mem_table_index = 0
        self.sst_index = [-1 for _ in range(opts.max_level)]

        self.wal: WriteAheadLog = None
        self.old_mem_tables = []
        self.nodes: [[Node]] = [[] for _ in range(opts.max_level)]

        if not os.path.exists(self.options.dir_path):
            os.mkdir(self.options.dir_path)
        self.load_mem_table()
        self.load_nodes()

    def insert(self, key, value):
        self.wal.write(Record(key, value))
        self.mem_table.insert(key, value)
        self.try_to_refresh_mem_table()

    def get(self, key):
        val, ok = self.mem_table.search(key)
        if ok:
            return val

        for old_mem_table in reversed(self.old_mem_tables):
            val, ok = old_mem_table.mem_table.search(key)
            if ok:
                return val

        for level in self.nodes:
            for node in reversed(level):
                val, ok = node.search(key)
                if ok:
                    return val
        return "false"

    def delete(self, key):
        self.wal.write(Record(key, None))
        self.mem_table.insert(key, None)
        self.try_to_refresh_mem_table()

    def check_mem_table_overflow(self):
        return self.mem_table.get_capacity() > self.options.mem_table_size

    async def handle_mem_table(self):
        if self.sst_index[0] == -1:
            self.sst_index[0] += 1
        sst = SSTable(self.options, 0, self.sst_index[0])
        # sst.write(self.mem_table)
        await asyncio.to_thread(sst.write,self.mem_table)
        self.nodes[0].append(Node(self.options, 0, self.sst_index[0]))
        for i in range(len(self.old_mem_tables)):
            if self.old_mem_tables[i] != self.mem_table:
                continue
            self.old_mem_tables = self.old_mem_tables[i + 1:]
        self.wal.delete()
        self.sst_index[0] += 1
        # await asyncio.to_thread(self.level_compact,0)
        # asyncio.run(self.level_compact(0))
        await self.level_compact(0)
    def new_mem_table(self):
        self.old_mem_tables.append(OldMemTable(self.mem_table, self.mem_table))
        asyncio.run(self.handle_mem_table())
        self.mem_table_index += 1
        self.mem_table = Tree()
        self.wal = WriteAheadLog(self.options.dir_path, self.mem_table_index)

    def try_to_refresh_mem_table(self):
        if self.check_mem_table_overflow():
            self.new_mem_table()

    def load_mem_table(self):
        wal_files = sorted([f for f in os.listdir(self.options.dir_path) if f.endswith('.wal')],
                           key=lambda x: parse_wal_file_path(x))
        if not wal_files:
            self.wal = WriteAheadLog(self.options.dir_path, self.mem_table_index)
            return
        for k, wal in enumerate(wal_files):
            mem_table_index = parse_wal_file_path(wal)
            wal = WriteAheadLog(self.options.dir_path, mem_table_index)
            records: [Record] = wal.read_all()
            curr_mem_table = Tree()
            for rec in records:
                curr_mem_table.insert(rec.key, rec.value)
            if k == len(wal_files) - 1:
                self.mem_table = curr_mem_table
                self.mem_table_index = mem_table_index
                self.wal = wal
            else:
                self.old_mem_tables.append(OldMemTable(mem_table_index, curr_mem_table))
                self.handle_mem_table()

    def load_nodes(self):
        sst_files = sorted([f for f in os.listdir(self.options.dir_path) if f.endswith('.sst')])
        sst_files.sort(key=lambda f: parse_sst_file_path(f))
        for sst_file in sst_files:
            level, sst_index = parse_sst_file_path(sst_file)
            self.nodes[level].append(Node(self.options, level, sst_index))
            self.sst_index[level] = sst_index

    def check_level_over_flow(self, level: int):
        if level == self.options.max_level:
            return False
        return len(self.nodes[level]) >= self.options.max_level_num

    async def level_compact(self, level):
        if not self.check_level_over_flow(level):
            return

        index = get_merge_data_left_right(len(self.nodes[level]))
        self.sst_index[level + 1] += 1
        new_sst = SSTable(self.options, level + 1, self.sst_index[level + 1])
        merged_tree = Tree()

        for node in self.nodes[level][:index + 1]:
            try:
                records = node.read_data()
                for record in records:
                    merged_tree.insert(record.key, record.value)
            except Exception as e:
                print(f"Error reading data from node {node.file_name}: {str(e)}")
                continue
            finally:
                node.delete()

        try:
            new_sst.write(merged_tree)
        except Exception as e:
            print(f"Error writing merged data to new SST: {str(e)}")

        self.nodes[level] = self.nodes[level][index + 1:]
        self.nodes[level + 1].append(Node(self.options, level + 1, self.sst_index[level + 1]))

        await self.level_compact(level + 1)

def test_lsm_tree():
    opts = Options("./data", mem_table_size=80)
    lsm_tree = LSMTree(opts)
    d = dict()
    for i in range(79):
        key, value = generate_key(i), generate_random_string(12)
        lsm_tree.insert(key, value)
        d[key] = value

    for i in range(1000):
        key, value = generate_key(i), generate_random_string(12)
        lsm_tree.insert(key, value)
        d[key] = value
    for i in range(900):
        key, value = generate_key(i), generate_random_string(12)
        lsm_tree.delete(key)
        d[key] = None

    for i in range(1000):
        key, _ = generate_key(i), generate_random_string(12)
        val = lsm_tree.get(key)
        # print(key, val)
        if i < 900:
            assert val == None
        else:
            assert len(val) == 12
        assert val == d[key]


def test_lsm_tree1():
    opts = Options("./data", mem_table_size=80)
    lsm_tree = LSMTree(opts)

    lsm_tree.mem_table.show()
    for i in range(1000):
        key, _ = generate_key(i), generate_random_string(12)
        val = lsm_tree.get(key)
        if i < 900:
            assert val == None
        else:
            assert len(val) == 12


def test_sst():
    opts = Options("./data", mem_table_size=80)
    lsm_tree = LSMTree(opts)
    print("测试！！！")
    lsm_tree.mem_table.show()
    for i in range(len(lsm_tree.nodes)):
        for j in range(len(lsm_tree.nodes[i]) - 1, -1, -1):
            print(lsm_tree.nodes[i][j].file_name, i, j)
            # node=lsm_tree.nodes[i][j]
            # for pi in node.parse_index:
            #     records = node._read_block_data(pi.block_offset, pi.block_length)
            #     for rec in records:
            #         print(node.file_name, rec)


def test_sst_info():
    options = Options("./data", )
    sst = SSTable(options, 0, 0, flag=True)
    print(sst, sst.file_name)
    sparse_index = sst.read_sparse_index()
    bloom_filter = BloomFilter(options.bloom_filter_size, options.bloom_filter_hash_num, options.bloom_filter_seed,
                               sst.read_bloom_filter())
    for i in range(100):
        key, value = generate_key(i), generate_random_string(12)
        ok = bloom_filter.check(key)
        print(key, ok)

    for si in sparse_index:
        records = sst.read_data_by_index(si.block_offset, si.block_length)
        for rec in records:
            print(rec)

    print(sst.read_data())
    print(sst.read_bloom_filter())
    print(sst.read_sparse_index())


def test_sst_data():
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
