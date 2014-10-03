from redis import Redis

from helpers import *
from structs import *

TOP_BLOCK = 'top_block'

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




class _RedisObject:

    def __init__(self, db: Database, path="", encodium_type=None):
        self._path = path
        self._db = db
        self._type = encodium_type


class RedisSet(_RedisObject):

    def __iter__(self):
        self._set_iterator = self._db.redis.smembers(self._path)
        return iter(self._set_iterator)

    def __next__(self):
        return self._set_iterator.__next__()


    def __len__(self):
        return self._db.redis.scard(self._path)

    def __contains__(self, item):
        return self._db.redis.sismember(self._path, item)

    def add(self, *args):
        return self._db.redis.sadd(self._path, *map(serialize_if_encodium, args))

    def remove(self, *args):
        return self._db.redis.srem(self._path, *map(serialize_if_encodium, args))

    def members(self):
        return_set = self._db.redis.smembers(self._path)
        if self._type is not None:
            return_set = {self._type(member) for member in return_set}
        return return_set


class RedisDictionary(_RedisObject):

    def __setitem__(self, key, value):
        assert not isinstance(key, Encodium)
        return self._db.redis.set(Database.concat(self._path, key), serialize_if_encodium(value))

    def __getitem__(self, item):
        value = self._db.redis.get(Database.concat(self._path, item))
        if value is None:
            return
        if self._type is not None:
            if issubclass(self._type, Encodium):
                return self._type.from_json(value.decode())
            return self._type(value)
        return value

    def __len__(self):
        return self._db.redis.scard(self._path)

    def __contains__(self, item):
        return self._db.redis.exists(Database.concat(self._path, item))
