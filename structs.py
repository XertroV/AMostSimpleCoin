from queue import PriorityQueue

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


# Block structure

class SimpleBlock(Encodium):
    links = List.Definition(Hash.Definition())
    work_target = Target.Definition()
    total_work = Integer32Bytes.Definition()
    timestamp = Timestamp.Definition()
    nonce = Nonce.Definition()

    def __init__(self, *args, **kwargs):
        self._primary_chain = None
        self._hash = None
        self._height = None
        self.linked_blocks = None
        self.tx = None
        super().__init__(*args, **kwargs)

    def __hash__(self):
        return self.hash

    def check(s, changed_attributes):
        assert s.work_target > 100000  # this is somewhat implied through the below
        assert s.coins_generated >= 0  # coins_generated and the assoc. fee can implicitly set an upper bound on the target
        assert len(s.links) <= 1  # todo: temporary: ensure there is only one linked block, also remove from set_linked_blocks

    # Graph

    def set_linked_blocks(self, blocks):
        # TODO: The asserts are here to sanity check for now, can maybe one day be removed
        assert all_true(lambda xs : xs[0].hash == xs[1], zip(blocks, self.links))

        sigma_diffs = [b.sigma_diff for b in blocks]
        assert all_true(lambda ys : ys[0] >= ys[1], zip(sigma_diffs[:-1], sigma_diffs[1:]))

        # todo: temporary: ensure only one linked block. see check() too
        assert len(blocks) <= 1

        self.linked_blocks = blocks

    # Properties

    @property
    def primary_chain(self):
        if self._primary_chain is None:
            if len(self.links) == 0:
                self._primary_chain = [self]
            else:
                self._primary_chain = self.linked_blocks[0].primary_chain + [self]
        return self._primary_chain

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

    @property
    def height(self):
        if self._height is None:
            if self.is_root:
                self._height = 0
            else:
                self._height = self.linked_blocks[0].height + 1
        return self._height


# State

class State:
    """ Super simple state device.
    Only functions are to add or subtract coins, and no checking is involved.
    All accounts are of 0 balance to begin with.
    """
    def __init__(self):
        self._state = {}

    def get(self, pub_x: ECPoint):
        return 0 if pub_x not in self._state else self._state[pub_x]

    def modify_balance(self, pub_x: ECPoint, value: MoneyAmount):
        self._state[pub_x] = self.get(pub_x) + value


# Graph Datastructs

class Orphanage:
    """ An Orphanage holds orphans.
    It acts as a priority queue, through put(), get(), etc. This is sorted by sigmadiff.
    For membership it acts as a set.
    """
    def __init__(self):
        self._priority_queue = PriorityQueue()
        self._set = set()
        self._removed = set()

    def __contains__(self, block: SimpleBlock):
        return False if block in self._removed else block in self._set

    def remove(self, block: SimpleBlock):
        if block not in self._set or block in self._removed:
            raise BlockNotFoundException()
        self._set.remove(block)
        self._removed.add(block)

    def _put_block(self, block: SimpleBlock):
        self._priority_queue.put((block.sigma_diff, block))

    def put(self, block: SimpleBlock):
        self._put_block(block)
        self._set.add(block)
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
        self._put_block(block)
        return block
