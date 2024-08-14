from src.sparse_index import SparseIndex, parse_multiple_indices


def test_sparse_index():
    sparse_index = SparseIndex("1", "100", 1, 100, 200, "test.txt")
    print(sparse_index)
    print(sparse_index.to_bytes())
    print(SparseIndex.from_bytes(sparse_index.to_bytes()))


def test_parse_multiple_indices():
    sparse_index1 = SparseIndex("1", "100", 1, 100, 200, "test1.txt")
    sparse_index2 = SparseIndex("101", "200", 2, 200, 300, "test2.txt")
    bytes_data = sparse_index1.to_bytes() + sparse_index2.to_bytes()
    indices = parse_multiple_indices(bytes_data)
    for index in indices:
        print(index)


test_parse_multiple_indices()
test_sparse_index()
