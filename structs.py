from queue import PriorityQueue
from collections import defaultdict

from encodium import *

from helpers import *

# Structs

class Integer32Bytes(Encodium):
    class Definition(Encodium.Definition):
        _encodium_type = int

        def check_value(self, value):
            return 0 <= value < MAX_32_BYTE_INT

ECPoint = Target = Hash = Integer32Bytes

class Integer8Bytes(Encodium):
    class Definition(Encodium.Definition):
        _encodium_type = int

        def check_value(self, value):
            return 0 <= value < MAX_8_BYTE_INT

MoneyAmount = Timestamp = Nonce = Integer8Bytes


# Associated Structures

class Signature(Encodium):
    r = ECPoint.Definition()
    s = ECPoint.Definition()
    pub_x = ECPoint.Definition()
    pub_y = ECPoint.Definition()
    msg_hash = Hash.Definition()

    def check(s, changed_attributes):
        assert valid_secp256k1_signature(s.pub_x, s.pub_y, s.msg_hash, s.r, s.s)

    @classmethod
    def from_secret_exponent_and_msg(cls, secret_exponent, msg):
        msg_hash = global_hash(msg)
        r, s = ecdsa.sign(ecdsa.generator_secp256k1, secret_exponent, msg_hash)
        x, y = ecdsa.public_pair_for_secret_exponent(ecdsa.generator_secp256k1, secret_exponent)
        return Signature(pub_x=x, pub_y=y, r=r, s=s, msg_hash=msg_hash)


class Transaction(Encodium):
    value = MoneyAmount.Definition()
    recipient = ECPoint.Definition()
    signature = Signature.Definition()



# Associated Structures

class Signature(Encodium):
    r = ECPoint.Definition()
    s = ECPoint.Definition()
    pub_x = ECPoint.Definition()
    pub_y = ECPoint.Definition()
    msg_hash = Hash.Definition()

    def check(s, changed_attributes):
        assert valid_secp256k1_signature(s.pub_x, s.pub_y, s.msg_hash, s.r, s.s)

    @classmethod
    def from_secret_exponent_and_msg(cls, secret_exponent, msg):
        msg_hash = global_hash(msg)
        r, s = ecdsa.sign(ecdsa.generator_secp256k1, secret_exponent, msg_hash)
        x, y = ecdsa.public_pair_for_secret_exponent(ecdsa.generator_secp256k1, secret_exponent)
        return Signature(pub_x=x, pub_y=y, r=r, s=s, msg_hash=msg_hash)


class Transaction(Encodium):
    value = MoneyAmount.Definition()
    recipient = ECPoint.Definition()
    signature = Signature.Definition()

    @property
    def total(self):
        return self.value


# Block structure

class SimpleBlock(Encodium):
    links = List.Definition(Hash.Definition())
    tx = Transaction.Definition(optional=True)
    coinbase = ECPoint.Definition(default=PUB_KEY_X_FOR_KNOWN_SE)
    work_target = Target.Definition()
    total_work = Integer32Bytes.Definition()
    state_hash = Hash.Definition(default=0)
    timestamp = Timestamp.Definition()
    nonce = Nonce.Definition()

    def __init__(self, *args, **kwargs):
        self._primary_chain = None
        self._hash = None
        self._height = None
        self.block_index = None
        self.tx = None
        super().__init__(*args, **kwargs)

    def __hash__(self):
        return self.hash

    def __gt__(self, other):
        return self.total_work > other.total_work

    def check(s, changed_attributes):
        assert s.work_target > 100000  # this is somewhat implied through the below
        assert s.coins_generated >= 0  # coins_generated and the assoc. fee can implicitly set an upper bound on the target
        assert len(s.links) <= 1  # todo: temporary: ensure there is only one linked block, also remove from set_linked_blocks

    # Graph

    def set_block_index(self, block_index):
        self.block_index = block_index

    # Properties

    @property
    def hash(self):
        if self._hash is None:
            self._hash = hash_block(self)
        return self._hash

    @property
    def sigma_diff(self):
        return self.total_work

    @property
    def coins_generated(self):
        return self.work_target - storage_fee(self)

    @property
    def acceptable_work(s):
        return ONE_WORK_HASH // s.hash > s.work_target

    @property
    def is_root(self):
        return len(self.links) == 0


# State

class State:
    """ Super simple state device.
    Only functions are to add or subtract coins, and no checking is involved.
    All accounts are of 0 balance to begin with.
    """
    def __init__(self):
        self._state = {}
        self._all_pubkeys = set()
        self._hash = None

    def get(self, pub_x: ECPoint):
        return 0 if pub_x not in self._all_pubkeys else self._state[pub_x]

    def modify_balance(self, pub_x: ECPoint, value: MoneyAmount):
        new_value = self.get(pub_x) + value
        assert new_value >= 0
        self._state[pub_x] = new_value
        self._all_pubkeys.add(pub_x)
        self._hash = None

    def full_state(self):
        return self._state

    @property
    def hash(self):
        # todo : note : this hash method relies on a map of pubkey_x's to balances. it'll fail with any other state
        if self._hash is None:
            all_pubkeys = list(self._all_pubkeys)
            all_pubkeys.sort()
            pair_to_bytes = lambda p, b1, b2 : (p[0].to_bytes(b1, 'big') + p[1].to_bytes(b2, 'big'))
            self._hash = global_hash(b''.join(map(lambda i : pair_to_bytes(i, 32, 8), zip(all_pubkeys, [self.get(i) for i in all_pubkeys]))))
        return self._hash


# Graph Datastructs

class Orphanage:
    """ An Orphanage holds orphans.
    It acts as a priority queue, through put(), get(), etc. This is sorted by sigmadiff.
    For membership it acts as a set.
    """
    def __init__(self):
        self._priority_queue = PriorityQueue()
        self._set = set()  # set of blocks
        self._parents_by_name = defaultdict(set)
        self._removed = set()

    def __contains__(self, block: SimpleBlock):
        return False if block in self._removed else block in self._set

    def remove(self, block: SimpleBlock):
        if block not in self._set or block in self._removed:
            raise BlockNotFoundException()
        self._set.remove(block)
        self._parents_by_name[block.links[0]].remove(block)
        self._removed.add(block)

    def _put_orphan(self, block: SimpleBlock):
        print('Orphanage, put block', block.to_json())
        self._priority_queue.put((block.total_work, block))

    def put(self, block: SimpleBlock):
        self._put_orphan(block)
        self._set.add(block)
        self._parents_by_name[block.links[0]].add(block)
        if block in self._removed:
            self._removed.remove(block)

    def _get_next_block(self):
        sigma_diff, block = self._priority_queue.get()
        while block in self._removed:
            sigma_diff, block = self._priority_queue.get()
        return block

    def get(self):
        block = self._get_next_block()
        self._set.remove(block)
        return block

    def visit(self):
        block = self._get_next_block()
        self._put_orphan(block)
        return block

    def children_of(self, parent_hash):
        if self._parents_by_name.get(parent_hash) is not None:  # need to use .get here because __getitem__ invokes the default
            return self._parents_by_name[parent_hash]  # safe to use __getitem__ here because we know it exists (we could anyway)
