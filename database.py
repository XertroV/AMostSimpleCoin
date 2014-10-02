from redis import Redis

from helpers import *
from structs import *

META_LATEST_INDEX = 'meta.latest.index'
INDEX_COUNT = 'index.count'
INDEX = 'index'

class Database:
    """ Database structure:
    legend: *key - value*

    meta.latest.index - <latest_index>: int

    index.count.<latest_index> - <count>
    <index>:0..<count - 1> - block hash
    <block_hash> - block.to_json()
    """

    def __init__(self):
        self.redis = Redis()

    @staticmethod
    def concat(*args):
        return '.'.join([str(a) for a in args])

    def _increment_index_count(self):
        return self.redis.incr(META_LATEST_INDEX)

    def create_new_index_of_chain(self, chain):
        index_number = self._increment_index_count()
        max_height = 0
        for height, block in enumerate(chain.head.primary_chain):
            self.redis.set(self._index_height_key(index_number, height), str(block.hash))
            max_height = height
        self.redis.set(self._index_count_key(index_number), str(max_height))
        return index_number

    def _index_count_key(self, index_number):
        return self.concat(INDEX_COUNT, index_number)

    def _index_height_key(self, index_number, height):
        self.concat(INDEX, str(index_number), str(height))

    def dump_chain(self, chain):
        for block in chain.all_nodes:
            if not self.redis.exists(str(block.hash)):
                self.redis.set(str(block.hash), block.to_json())

    def purge(self, offset: int):
        pass

    def get_latest_index(self):
        def init():
            self.redis.set(META_LATEST_INDEX, 0)
            self.redis.set(self._index_count_key(0), 0)

        if not self.redis.exists(META_LATEST_INDEX):  # not initialized
            init()

        index = []
        index_number = self.redis.get(META_LATEST_INDEX)
        num_hashes = self.redis.get(self._index_count_key(index_number))
        if num_hashes == None:
            init()
            num_hashes = 0
        for height in range(num_hashes):
            index.append(self.redis.get(self._index_height_key(index_number, height)))
        return index

    def get_blocks(self, blocks):
        '''
        :param blocks: list of hashes
        :return: list of Block objects (encodium)
        '''
        return [SimpleBlock.from_json(self.redis.get(str(block_hash))) for block_hash in blocks]