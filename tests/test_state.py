from unittest import TestCase

from database import Database, State
from helpers import *


class TestState(TestCase):
    def setUp(self):
        self.db = Database(db_num=15)
        self.r = self.db.redis
        self.r.flushall()
        self.state = State(self.db)

        self.state.modify_balance(PUB_KEY_X_FOR_KNOWN_SE, 20)

    def test_get(self):
        assert_equal(20, self.state.get(PUB_KEY_X_FOR_KNOWN_SE))

    def test_modify_balance(self):
        original_balance = self.state.get(PUB_KEY_X_FOR_KNOWN_SE)
        self.state.modify_balance(PUB_KEY_X_FOR_KNOWN_SE, 99)
        assert_equal(99 + original_balance, self.state.get(PUB_KEY_X_FOR_KNOWN_SE))
        self.state.modify_balance(PUB_KEY_X_FOR_KNOWN_SE, -99)
        assert_equal(original_balance, self.state.get(PUB_KEY_X_FOR_KNOWN_SE))

    def test_full_state(self):
        assert_equal({PUB_KEY_X_FOR_KNOWN_SE: 20}, self.state.full_state())

    def test_all_keys(self):
        assert_equal({PUB_KEY_X_FOR_KNOWN_SE}, self.state.all_keys())

    def test_backups(self):
        original_balance = self.state.get(PUB_KEY_X_FOR_KNOWN_SE)
        self.state.backup()
        self.state.modify_balance(PUB_KEY_X_FOR_KNOWN_SE, 99)
        assert_equal(99 + original_balance, self.state.get(PUB_KEY_X_FOR_KNOWN_SE))
        self.state.restore_backup()
        assert_equal(original_balance, self.state.get(PUB_KEY_X_FOR_KNOWN_SE))

    def test_hash(self):
        _hash = global_hash(PUB_KEY_X_FOR_KNOWN_SE.to_bytes(32, 'big') + (20).to_bytes(8, 'big'))
        assert_equal(_hash, self.state.hash)

