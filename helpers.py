from hashlib import sha256
from queue import PriorityQueue, Empty

import pycoin.ecdsa as ecdsa

from encodium import Encodium

from SSTT.utils import fire, nice_sleep

# Constants

ONE_WORK_HASH = MAX_32_BYTE_INT = 256 ** 32 - 1
MAX_8_BYTE_INT = 256 ** 8 - 1

FEE_CONSTANT = 10000  # 8000000  # Arbitrary-ish


# Functions

def is_32_bytes(i):
    return 0 <= i < 256 ** 32

def is_4_bytes(i):
    return 0 <= i < 256 ** 4

def all_true(f, l):
    return False not in map(f, l)

def global_hash(msg: bytes):
    return int.from_bytes(sha256(msg).digest(), 'big')

def hash_block(block: Encodium):
    return global_hash(bytes(block.serialize()))

def hash_target_to_work_target(hash_target):
    return ONE_WORK_HASH // hash_target

def work_target_to_hash_target(work_target):
    return ONE_WORK_HASH // work_target

def exclude_from(a, b):  # operates on paths
    return [i for i in a if i not in set(b)]

def get_n_from_pq_and_block(limit: int, pq: PriorityQueue, timeout=None):
    return_list = [pq.get(block=True, timeout=timeout)]
    try:
        for i in range(limit - 1):
            return_list.append(pq.get(block=False))
    except Empty:
        pass
    return return_list

def wait_for_all_threads_to_finish(threads):
    for t in threads:
        t.join()

# Crypto Helpers

def valid_secp256k1_signature(x, y, msg, r, s):
    return ecdsa.verify(ecdsa.generator_secp256k1, (x, y), global_hash(msg), (r, s))


# Network-y functions

def storage_fee(block):
    return len(block.to_json()) * FEE_CONSTANT



# Exceptions

InvalidBlockException = BlockNotFoundException = Exception

