from bintrees import RBTree

import threading

from util import generate_key, generate_random_string


class Tree:
    # 实例化
    def __init__(self):
        self.tree = RBTree()
        self.lock = threading.RLock()

    # 插入
    def insert(self, k, v):
        with self.lock:
            self.tree.insert(k, v)

    # 查找
    def search(self, k):
        with self.lock:
            try:
                return self.tree[k],True
            except KeyError:
                return None,False

    # 显示
    def show(self):
        with self.lock:
            for key, value in self.tree.items():
                print(f"Key: {key}, Value: {value}")

    def fold(self, func):
        with self.lock:
            for key, value in self.tree.items():
                if not func(key, value):
                    return False
            return True

    def items(self):
        with self.lock:
            return self.tree.items()

    # 获取容量
    def get_capacity(self):
        with self.lock:
            return self.tree.count

    # 实现合并
    def merge(self, other):
        with self.lock:
            self.tree.update(other.tree)
