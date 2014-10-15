from unittest import TestCase


from database import Database, PrimaryChain
from helpers import assert_equal


db = Database(db_num=15)
db.redis.flushall()


class TestPrimaryChain(TestCase):
    def setUp(self):
        db.redis.flushall()

        self.p = PrimaryChain(db)

        hashes = [1, 2, 3, 4, 5, 6, 7]

        self.p.append_hashes(hashes[:4])

    def test_get_all(self):
        assert_equal(self.p.get_all(), [1,2,3,4])

    def test_append_hashes(self):
        self.p.append_hashes([5,6,7])
        assert_equal([1,2,3,4,5,6,7], self.p.get_all())

    def test_remove_hashes(self):
        self.p.remove_hashes([4,3,2])
        assert_equal([1], self.p.get_all())
        assert_equal(True, self.p.contains(1))
        assert_equal(False, self.p.contains(2))


    def test_contains(self):
        assert_equal(True, self.p.contains(1))
        assert_equal(True, self.p.contains(2))
        assert_equal(True, self.p.contains(3))
        assert_equal(True, self.p.contains(4))
        assert_equal(False, self.p.contains(5))