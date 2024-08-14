class Meta:
    def __init__(self, block_length, parse_index_length, filter_length):
        self.block_length = block_length
        self.parse_index_length = parse_index_length
        self.filter_length = filter_length

    def set_parse_index_length(self, parse_index_length):
        self.parse_index_length = parse_index_length

    def set_filter_length(self, filter_length):
        self.filter_length = filter_length

    def __str__(self):
        return f"block_length: {self.block_length}, parse_index_length: {self.parse_index_length}, filter_length: {self.filter_length}"

    def to_bytes(self):
        return self.block_length.to_bytes(4, byteorder='big') + self.parse_index_length.to_bytes(4,
                                                                                                 byteorder='big') + self.filter_length.to_bytes(
            4, byteorder='big')

    def from_bytes(self, data):
        self.block_length = int.from_bytes(data[:4], byteorder='big')
        self.parse_index_length = int.from_bytes(data[4:8], byteorder='big')
        self.filter_length = int.from_bytes(data[8:12], byteorder='big')


def test_meta():
    meta = Meta(1784, 1274, 6478367)
    print(meta.to_bytes())
    meta.from_bytes(meta.to_bytes())
    print(meta)
