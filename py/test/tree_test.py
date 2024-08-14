# 进行测试
from src.bloom_filter import BloomFilter
from src.mem_table import Tree
from util import generate_key, generate_random_string


def test_rbtree():
    rbtree = Tree()
    for i in range(10):
        rbtree.insert(generate_key(i), generate_random_string())
    rbtree.show()

    print("Folded:")

    def print_key_value(key, value):
        print(f"Key: {key}, Value: {value}")
        return True

    rbtree.fold(print_key_value)
    print("Capacity: ", rbtree.get_capacity())
    print("Size: ", rbtree.get_capacity())

    rbtree1 = Tree()
    for i in range(10, 100):
        rbtree1.insert(generate_key(i), generate_random_string())

    rbtree.merge(rbtree1)
    rbtree.show()


# Run the test cases
# test_rbtree()

def test_bloom_filter():
    bf = BloomFilter(1024, 3, 1)
    rbtree1 = Tree()
    for i in range(100):
        key, value = generate_key(i), generate_random_string()
        rbtree1.insert(key, value)
        bf.add(key)
    byte_data = bf.to_bytes()
    print(bytes)
    # bf1=BloomFilter(1024,3,1,byte_data)
    bf.from_bytes(byte_data)
    for i in range(101):
        key, value = generate_key(i), generate_random_string()
        print("bf1.check(key)", bf.check(key))


test_bloom_filter()
