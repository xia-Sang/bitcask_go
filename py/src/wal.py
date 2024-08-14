import os
import threading

from record import Record
from util import get_wal_file_path


class WriteAheadLog:
    def __init__(self, log_dir, index_id):
        self.log_dir = log_dir
        self.log_file = get_wal_file_path(log_dir, index_id)
        self.lock = threading.Lock()  #读写锁
        self.read_lock = threading.Condition(self.lock)
        self.write_lock = threading.Condition(self.lock)
        self.readers = 0
        self.writer = False
        self._init_log()

    def _init_log(self):
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
        if not os.path.exists(self.log_file):
            with open(self.log_file, 'w') as f:
                f.write("")

    def write(self, record: Record):
        with self.write_lock:
            while self.writer or self.readers > 0:
                self.write_lock.wait()
            self.writer = True
            with open(self.log_file, 'ab') as f:
                # 写入长度之后再写入数据
                data = record.to_bytes()
                f.write(len(data).to_bytes(4, 'big'))  #写入二进制长度
                f.write(data)
            with open(self.log_file, 'ab') as f:
                f.flush()
            self.writer = False
            self.write_lock.notify_all()

    def read_all(self) -> [Record]:
        with self.read_lock:
            while self.writer:
                self.read_lock.wait()
            self.readers += 1
            records = []
            with open(self.log_file, 'rb') as f:
                # 根据写入来读取
                while True:
                    length = f.read(4)
                    if not length:
                        break
                    data = f.read(int.from_bytes(length, 'big'))
                    record = Record.from_bytes(data)
                    records.append(record)
            self.readers -= 1
            if self.readers == 0:
                self.write_lock.notify_all()
            return records

    def clear(self):
        with self.write_lock:
            while self.writer or self.readers > 0:
                self.write_lock.wait()
            self.writer = True
            with open(self.log_file, 'w') as f:
                f.write("")
            self.writer = False
            self.write_lock.notify_all()

    def get_wal_size(self):
        # 注意锁
        with self.read_lock:
            while self.writer:
                self.read_lock.wait()
            self.readers += 1
            size = os.path.getsize(self.log_file)
            self.readers -= 1
            if self.readers == 0:
                self.write_lock.notify_all()
            return size

    # 删除wal文件
    def delete(self):
        with self.write_lock:
            while self.writer or self.readers > 0:
                self.write_lock.wait()
            self.writer = True
            os.remove(self.log_file)
            self.writer = False
            self.write_lock.notify_all()
