from unittest import TestCase

from database import Database, RedisFlag
from helpers import assert_equal

db = Database(db_num=15)
db.redis.flushall()

class TestRedisFlag(TestCase):
    def setUp(self):
        self.flag = RedisFlag(db, 'test_flag')

    def test_set_true(self):
        self.flag.set_true()
        assert_equal(True, self.flag.is_true)

    def test_set_false(self):
        self.flag.set_false()
        assert_equal(False, self.flag.is_true)