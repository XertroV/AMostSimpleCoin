from redis import Redis

from helpers import *
from structs import *
import lua_helpers

TOP_BLOCK = 'top_block'


class strbytes(str):  # it plays nice with redis now -- encodes and decodes bytes <-> strings
    def __new__(self, object=b'', encoding='utf-8', errors='strict'):
        return str.__new__(self, object, encoding, errors)


class Database:

    def __init__(self, db_num=0):
        self.redis = Redis(db=db_num)

    @staticmethod
    def concat(*args):
        return '.'.join([str(a) for a in args])

    def get_blocks(self, blocks):
        '''
        :param blocks: list of hashes
        :return: list of Block objects (encodium)
        '''
        return [SimpleBlock.from_json(self.redis.get(str(block_hash)).decode()) for block_hash in blocks]

    def set_kv(self, key, value):
        return self.redis.set(key, serialize_if_encodium(value))

    def get_kv(self, key, optional_type=None):
        result = self.redis.get(key)
        print('getting', key, result)
        if optional_type is not None and result is not None:
            if issubclass(optional_type, Encodium):
                return optional_type.from_json(result.decode())
            return optional_type(result)
        return result


concat = Database.concat


class _RedisObject:

    def __init__(self, db: Database, path="", value_type=None):
        self._path = path
        self._db = db
        self._r = db.redis
        self._type = value_type

    def reset(self):
        return self._r.delete(self._path)



class RedisFlag(_RedisObject):

    def set_true(self):
        self._r.set(self._path, True)

    def set_false(self):
        self._r.set(self._path, False)

    @property
    def is_true(self):
        if self._r.exists(self._path):
            return self._r.get(self._path) == b'True'
        return False

def generate(o):
    while True:
        yield o

def ensure_type(f):
    # f should be a class method
    def inner(instance: _RedisObject, *args, **kwargs):
        r = f(instance, *args, **kwargs)
        if isinstance(r, bytes):
            return parse_type_over(instance._type, r)
        return many_parse_type_over(generate(instance._type), r)
    return inner


class RedisList(_RedisObject):

    @ensure_type
    def __getitem__(self, item):
        if isinstance(item, slice):
            start, stop = item.start, item.stop
            if item.step != None:
                raise NotImplemented("Steps in slices is not implemented for RedisList")
            if stop == None:
                stop = self._r.llen(self._path)
            if start == None:
                start = 0
            return self._r.lrange(self._path, start, stop - 1)  # need to subtract 1 because redis is inclusive, but python exclusive
        return self._r.lindex(self._path, item)

    def __setitem__(self, key, value):
        return self._r.lset(self._path, key, serialize_if_encodium(value))

    def prepend(self, *values):
        return self._r.lpush(self._path, *values)

    def append(self, *values):
        return self._r.rpush(self._path, *values)

    def lpop(self):
        return parse_type_over(self._type, self._r.lpop(self._path))

    def rpop(self):
        return parse_type_over(self._type, self._r.rpop(self._path))


class RedisSet(_RedisObject):

    def __iter__(self):
        self._set_iterator = self.members()
        return iter(self._set_iterator)

    def __next__(self):
        return self._set_iterator.__next__()

    def __len__(self):
        return self._db.redis.scard(self._path)

    def __contains__(self, item):
        return self._db.redis.sismember(self._path, serialize_if_encodium(item))

    def add(self, *args):
        return self._r.sadd(self._path, *map(serialize_if_encodium, args))

    def remove(self, *args):
        return self._r.srem(self._path, *map(serialize_if_encodium, args))

    def members(self):
        return_set = self._db.redis.smembers(self._path)
        return {parse_type_over(self._type, member) for member in return_set}


class RedisHashMap(_RedisObject):

    def __init__(self, db: Database, path="", key_type=None, value_type=None):
        self._path = path
        self._db = db
        self._r = db.redis
        self._key_type = key_type
        self._value_type = value_type
        assert str not in (key_type, value_type)  # for the moment, should use strbytes

    def __setitem__(self, key, value):
        return self._db.redis.hset(self._path, key, serialize_if_encodium(value))

    def __getitem__(self, item):
        return parse_type_over(self._value_type, self._db.redis.hget(self._path, item))

    def __len__(self):
        return self._db.redis.hlen(self._path)

    def __contains__(self, item):
        return self._db.redis.hexists(self._path, item)

    def get_all(self):
        items = self._r.hgetall(self._path).items()
        return dict([many_parse_type_over((self._key_type, self._value_type), i) for i in items])

    def all_keys(self):
        return set(map(lambda k : parse_type_over(self._key_type, k), self._db.redis.hkeys(self._path)))


# State


class State(_RedisObject):
    """ Super simple state device.
    Only functions are to add or subtract coins, and no checking is involved.
    All accounts are of 0 balance to begin with.
    """
    def __init__(self, db: Database, path="state", backup_path="backup_state"):
        super().__init__(db, path)

        self.lock = threading.Lock()

        self._backup_path = backup_path
        self._state = RedisHashMap(db, self._path, int, int)  # ECPoint, MoneyAmount)    <- use those types again when encodium is patched
        self._hash = None

        self._mod_balance = lua_helpers.get_mod_balance(self._db.redis, self._path)
        self._get_balance = lua_helpers.get_get_balance(self._db.redis, self._path)
        self._backup_state = lua_helpers.get_backup_state_function(self._r, self._path, self._backup_path)
        self._restore_backup = lua_helpers.get_restore_backup_state_function(self._r, self._path, self._backup_path)

    def get(self, pub_x: ECPoint):
        return int(self._get_balance(keys=[pub_x]))

    def modify_balance(self, pub_x: ECPoint, value: MoneyAmount, r: Redis=None):  # todo : write modify_many_balances(...)
        #assert new_value >= 0  # todo, we probably shouldn't validate this here?
        self.modify_many_balances([pub_x], [value])

    def modify_many_balances(self, pub_xs, values):
        self._mod_balance(keys=pub_xs, args=values)
        self._hash = None

    def full_state(self):
        return self._state.get_all()

    def all_keys(self):
        return self._state.all_keys()

    def backup_to(self, backup_path):
        self._backup_state(keys=[backup_path])

    def restore_backup_from(self, backup_path):
        self._restore_backup(keys=[backup_path])

    def reset(self):
        if self._r.exists(self._path):
            self._r.delete(self._path)
        self._state[0] = 0  # allows backing up an "empty" state


    @property
    def hash(self):
        # todo : note : this hash method relies on a map of pubkey_x's to balances. it'll fail with any other state
        if self._hash is None:
            all_pubkeys = list(self.all_keys())
            all_pubkeys.sort()
            pair_to_bytes = lambda p, b1, b2 : (p[0].to_bytes(b1, 'big') + p[1].to_bytes(b2, 'big'))
            self._hash = global_hash(b''.join(map(lambda i : pair_to_bytes(i, 32, 8), zip(all_pubkeys, [self.get(i) for i in all_pubkeys]))))
        return self._hash


class PrimaryChain:
    """ PrimaryChain is a list of hashes representing the primary chain.
    """
    def __init__(self, db, path="primary_chain"):
        self._db = db
        self._r = r = db.redis
        self._path = path
        self._type = int

        self._chain = RedisList(db, path, int)

        self._get_all = lua_helpers.get_primary_chain_get_all(r, path)
        self._remove = lua_helpers.get_primary_chain_remove(r, path)
        self._append = lua_helpers.get_primary_chain_append(r, path)
        self._contains = lua_helpers.get_primary_chain_contains(r, path)

    @ensure_type
    def get_all(self):
        return self._get_all()

    def append_hashes(self, block_hashes):
        self._append(keys=block_hashes)

    def remove_hashes(self, block_hashes):
        self._remove(keys=block_hashes)

    def contains(self, block_hash):
        return bool(self._contains(keys=[block_hash]))


class Orphanage:
    """ An Orphanage holds orphans.
    It acts as a priority queue, through put(), get(), etc. This is sorted by sigmadiff.
    For membership it acts as a set.

    Requirements:
    membership test for orphans (1)
    retrieval of orphans (2)
    reverse lookup (of linking blocks) (3 and 4) (expensive)

    Redis Particulars:
     (ser_ indicates a serialized version of a block )
     {variable}

        Internal Structures:
            1. set(block_hashes)
            2. hash_map(block_hash -> ser_block)
            3. hash_map(block_hash -> {linking_block_hashes})
            4. {linking_block_hashes} -> set(linking_block_hashes)

    """
    def __init__(self, db, path="orphanage"):
        self._db = db
        self._r = r = db.redis
        self._path = path
        self._get = lua_helpers.get_orphanage_get(r, path)
        self._add = lua_helpers.get_orphanage_add(r, path)
        self._contains = lua_helpers.get_orphanage_contains(r, path)
        self._remove = lua_helpers.get_orphanage_remove(r, path)
        self._linking_to = lua_helpers.get_orphanage_linking_to(r, path)

    def __contains__(self, block: SimpleBlock):
        return self._contains(keys=[block.hash])

    def remove(self, block: SimpleBlock):
        return self._remove(keys=[block.hash, block.links[0]])

    def add(self, block: SimpleBlock):
        return self._add(keys=[block.to_json(), block.hash, block.links[0]])

    def get(self, block_hash):
        block = self._get(keys=[block_hash])
        if block is not None:
            block = SimpleBlock.from_json(block.decode())
        return block

    def children_of(self, parent_hash):
        # decode bytes into block objects
        print(self._linking_to(keys=[parent_hash]))
        print({int(i.decode()) for i in self._linking_to(keys=[parent_hash])})
        return {int(i.decode()) for i in self._linking_to(keys=[parent_hash])}

    def contains_block_hash(self, block_hash):
        return self._contains(keys=[block_hash])
