from unittest import TestCase

from database import Database, RedisList
from helpers import assert_equal

db = Database(db_num=15)
db.redis.flushall()

class TestRedisFlag(TestCase):
    def setUp(self):
        self.l = RedisList(db, 'test_flag', int)

    def test_list(self):
        self.l.append(2,3)
        self.l.prepend(1)

        # get
        assert_equal(self.l[0], 1)
        assert_equal(self.l[2], 3)

        # set
        self.l[0] = 0
        self.l[2] = 0
        assert_equal(self.l[0], 0)
        assert_equal(self.l[2], 0)
        self.l[0] = 1
        self.l[2] = 3

        # simple slices
        assert_equal(self.l[0:2], [1,2])
        assert_equal(self.l[0:], [1,2,3])
        assert_equal(self.l[:], [1,2,3])
        assert_equal(self.l[:2], [1,2])

        # get via pops
        assert_equal(self.l.lpop(), 1)
        assert_equal(self.l.rpop(), 3)
        assert_equal(self.l.lpop(), 2)