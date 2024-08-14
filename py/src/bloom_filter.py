import hashlib
import bitarray


class BloomFilter:
    def __init__(self, size, hash_count, seed, bit_array=None):
        self.size = size
        self.hash_count = hash_count
        self.seed = seed
        if bit_array:
            self.bit_array = bitarray.bitarray()
            self.bit_array.frombytes(bit_array)
        else:
            self.bit_array = bitarray.bitarray(size)
            self.bit_array.setall(0)

    def to_bytes(self):
        return self.bit_array.tobytes()

    def from_bytes(self, bytes_data):
        self.bit_array = bitarray.bitarray()
        self.bit_array.frombytes(bytes_data)

    def _hashes(self, item):
        result = []
        for i in range(self.hash_count):
            combined_seed = self.seed + i
            hash_val = int(hashlib.sha256((str(combined_seed) + item).encode()).hexdigest(), 16)
            result.append(hash_val % self.size)
        return result

    def add(self, item):
        for hash_val in self._hashes(item):
            self.bit_array[hash_val] = 1

    def check(self, item):
        return all(self.bit_array[hash_val] for hash_val in self._hashes(item))

    def reset(self):
        self.bit_array.setall(0)
