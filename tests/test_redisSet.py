from unittest import TestCase

from database import Database, RedisSet
from helpers import *

db = Database(db_num=15)
db.redis.flushdb()

class TestRedisSet(TestCase):

    def setUp(self):
        self.s = RedisSet(db, "test_set", int)

        for i in range(1, 10):
            self.s.add(i)

    def test_remove(self):
        self.s.remove(1, 2, 3)
        assert_equal(len(self.s), 6)
        assert_equal(3 not in self.s, True)
        assert_equal(4 in self.s, True)

    def test_members(self):
        assert_equal(set(range(1, 10)), self.s.members())

    def test_len(self):
        assert_equal(9, len(self.s))

    def test_contains(self):
        self.s.remove(1, 2, 3)
        assert_equal(len(self.s), 6)
        assert_equal(3 not in self.s, True)
        assert_equal(4 in self.s, True)


    def test_iterate(self):
        stuff = []

        for i in self.s:
            stuff.append(i)

        sstuff = set(stuff)
        assert_equal(len(stuff), len(sstuff))
        assert_equal(sstuff, set(range(1,10)))

