import random
import string
import os

def generate_random_number():
    return random.randint(0, 100)

def generate_random_string(length=10):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))

def generate_key(key):
    return "test_key_" + str(key)

def get_wal_file_path(dir_path, key):
    return os.path.join(dir_path, "{0:09d}.wal".format(key))

def parse_wal_file_path(wal_file_path):
    return int(os.path.basename(wal_file_path).split('.')[0])

# def get_sst_file_path(level, sst_index):
    # return  "{0:04}_{1:03}.sst".format(level, sst_index)
def get_sst_file_path(level, sst_index):
    return  "{}_{}.sst".format(level, sst_index)
def parse_sst_file_path(sst_file_path):
    return int(sst_file_path.split('_')[0]), int(sst_file_path.split('_')[1].split('.')[0])

def get_merge_data_left_right(length:int):
    return random.randint(0, length - 1)