# 定义record
# keySize: 4 data
# key: keySize data
# valueSize: 4 data
# value: valueSize data
# checksum: 4 data
# 总长度：16 data
# 如果valueSize为0，则表示删除操作，此时value为空

import struct
import zlib

class Record:
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __str__(self):
        return f"Record(key={self.key}, value={self.value})"
    # 将record转换为bytes
    def to_bytes(self):
        key_bytes = self.key.encode('utf-8')
        value_bytes = self.value.encode('utf-8') if self.value else b''
        key_size = len(key_bytes)
        value_size = len(value_bytes)
        
        # 使用 zlib.crc32 计算 checksum
        checksum = zlib.crc32(struct.pack(f'!I{key_size}sI{value_size}s', key_size, key_bytes, value_size, value_bytes)) & 0xffffffff

        # 使用 struct 打包
        return struct.pack(f'!I{key_size}sI{value_size}sI', key_size, key_bytes, value_size, value_bytes, checksum)

    @staticmethod
    def from_bytes(bytes_data):
        # 解析单个record
        key_size = struct.unpack('!I', bytes_data[:4])[0]
        key = bytes_data[4:4 + key_size].decode('utf-8')
        value_size_offset = 4 + key_size
        value_size = struct.unpack('!I', bytes_data[value_size_offset:value_size_offset + 4])[0]
        value = bytes_data[value_size_offset + 4:value_size_offset + 4 + value_size].decode('utf-8') if value_size > 0 else None
        checksum_offset = value_size_offset + 4 + value_size
        checksum = struct.unpack('!I', bytes_data[checksum_offset:checksum_offset + 4])[0]
        calculated_checksum = zlib.crc32(bytes_data[:checksum_offset]) & 0xffffffff
        if checksum != calculated_checksum:
            raise ValueError("Checksum does not match!")
        return Record(key, value)

def from_bytes_multiple(bytes_data):
    # 解析一组record
    records = []
    offset = 0
    while offset < len(bytes_data):
        key_size = struct.unpack('!I', bytes_data[offset:offset + 4])[0]
        offset += 4
        key = bytes_data[offset:offset + key_size].decode('utf-8')
        offset += key_size
        value_size = struct.unpack('!I', bytes_data[offset:offset + 4])[0]
        offset += 4
        value = bytes_data[offset:offset + value_size].decode('utf-8') if value_size > 0 else None
        offset += value_size
        checksum = struct.unpack('!I', bytes_data[offset:offset + 4])[0]
        offset += 4
        calculated_checksum = zlib.crc32(bytes_data[offset - (4 + key_size + 4 + value_size + 4):offset - 4]) & 0xffffffff
        if checksum != calculated_checksum:
            raise ValueError("Checksum does not match!")
        records.append(Record(key, value))
    return records


# # 进行测试
# if __name__ == "__main__":
#     # 创建一个Record对象
#     record = Record("test_key", "test_value")
    
#     # 将Record对象转换为bytes
#     record_bytes = record.data()
#     print(f"Record data: {record_bytes}")
    
#     # 从bytes转换回Record对象
#     restored_record = Record.from_bytes(record_bytes)
#     print(f"Restored Record - Key: {restored_record.key}, Value: {restored_record.value}")
    
#     # 测试删除操作
#     delete_record = Record("test_key", "")
#     delete_record_bytes = delete_record.data()
#     print(f"Delete Record data: {delete_record_bytes}")
    
#     restored_delete_record = Record.from_bytes(delete_record_bytes)
#     print(f"Restored Delete Record - Key: {restored_delete_record.key}, Value: {restored_delete_record.value}")
