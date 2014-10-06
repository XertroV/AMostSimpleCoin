from hashlib import sha256
from queue import Empty
from asyncio import PriorityQueue
import threading
import time, pprint

import pycoin.ecdsa as ecdsa

from encodium import Encodium

# Constants

ONE_WORK_HASH = MAX_32_BYTE_INT = 256 ** 32 - 1
MAX_8_BYTE_INT = 256 ** 8 - 1

FEE_CONSTANT = 1000  # 8000000  # Arbitrary-ish


# Functions

def is_32_bytes(i):
    return 0 <= i < 256 ** 32

def is_4_bytes(i):
    return 0 <= i < 256 ** 4

def all_true(f, l):
    return False not in map(f, l)

def assert_equal(a, b):
    if a != b:
        raise AssertionError('%s not equal to %s' % (a, b))

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

def zero_if_none(thing):
    if thing is None:
        return 0
    return thing

# Serialization stuff

def serialize_if_encodium(o):
    if isinstance(o, Encodium):
        return o.to_json()
    return o

def parse_type_over(type, value):
    if type is not None:
        if issubclass(type, Encodium):
            return type.from_json(value.decode())
        return type(value)
    return value

def many_parse_type_over(types, values):
    return list(map(lambda p : parse_type_over(p[0], p[1]) ,zip(types, values)))

def replace_type_with_decode_if_str(type):
    if type == str:
        return lambda i: i.decode()
    return type

# Threads

def wait_for_all_threads_to_finish(threads):
    for t in threads:
        t.join()

def fire(target, args=(), kwargs={}):
    t = threading.Thread(target=target, args=args, kwargs=kwargs)
    t.start()
    return t

pp = pprint.PrettyPrinter(indent=4).pprint


def nice_sleep(object, seconds):
    '''
    This sleep is nice because it pays attention to an object's ._shutdown variable.
    :param object: some object with a _shutdown variable
    :param seconds: seconds in float, int, w/e
    :return: none
    '''
    for i in range(int(seconds * 10)):
        time.sleep(0.1)
        if object.is_shutdown:
            break

# Crypto Helpers

def valid_secp256k1_signature(x, y, msg, r, s):
    return ecdsa.verify(ecdsa.generator_secp256k1, (x, y), global_hash(msg), (r, s))

def pubkey_for_secret_exponent(exponent):
    return ecdsa.public_pair_for_secret_exponent(ecdsa.generator_secp256k1, exponent)

PUB_KEY_FOR_KNOWN_SE = ecdsa.public_pair_for_secret_exponent(ecdsa.generator_secp256k1, 1)
PUB_KEY_X_FOR_KNOWN_SE = PUB_KEY_FOR_KNOWN_SE[0]

# Network-y functions

def storage_fee(block):
    return (len(block.to_json()) - len(str(block.state_hash))) // 32 * FEE_CONSTANT



# Exceptions

InvalidBlockException = BlockNotFoundException = Exception

