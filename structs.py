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


