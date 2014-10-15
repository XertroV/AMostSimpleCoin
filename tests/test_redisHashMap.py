from unittest import TestCase

from database import Database, RedisHashMap, strbytes
from helpers import *

db = Database(db_num=15)

class TestRedisHashMap(TestCase):
    def setUp(self):
        self.hm = RedisHashMap(db, "test_path", int, strbytes)

        self.hm[1] = 'a'
        self.hm[2] = 'b'
        self.hm[0] = 'c'

    def test_get_all(self):
        assert_equal({0: 'c', 1: 'a', 2: 'b'}, self.hm.get_all())

    def test_all_keys(self):
        assert_equal({0, 1, 2}, self.hm.all_keys())

    def test___len__(self):
        assert_equal(3, len(self.hm))