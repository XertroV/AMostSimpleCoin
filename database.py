from redis import Redis

from helpers import *
from structs import *
import lua_helpers

TOP_BLOCK = 'top_block'


class strbytes(str):  # it plays nice with redis now -- encodes and decodes bytes <-> strings
    def __new__(self, object=b'', encoding='utf-8', errors='strict'):
        return str.__new__(self, object, encoding, errors)


class Database:
    """ Database structure:
    legend: *key - value*

    meta.latest.index - <latest_index>: int

    index.count.<latest_index> - <count>
    <index>:0..<count - 1> - block hash
    <block_hash> - block.to_json()
    """

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


class RedisList(_RedisObject):
    pass


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
