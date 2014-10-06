from copy import deepcopy
import traceback
import asyncio

from WSSTT import Network

from structs import *
from helpers import *
from seeker import Seeker
from database import Database, RedisFlag, RedisHashMap, RedisSet, State

# TODO : figure out best where to hook DB in
TOP_BLOCK = 'top_block'

class Chain:
    def __init__(self, root: SimpleBlock, db: Database, p2p: Network):
        self._db = db
        self._p2p = p2p
        self.root = root

        self.state = State(self._db)

        self.orphans = Orphanage()
        self.all_node_hashes = RedisSet(db, 'all_nodes')
        self.block_index = RedisHashMap(db, 'block_index', int, SimpleBlock)
        self.block_heights = RedisHashMap(db, 'block_heights', int, int)
        self.heights = RedisHashMap(db, 'heights', int)
        self._initialized = RedisFlag(db, 'initialized')

        self.head = self._get_top_block()

        self.seeker = Seeker(self, self._p2p)  # format: (total_work, block_hash) - get early blocks first
        self.currently_seeking = set()

        if not self._initialized.is_true:
            self.first_initialize()
            self._initialized.set_true()

    def first_initialize(self):
        self.heights[0] = self.root.hash
        self.block_heights[self.root.hash] = 0
        self.block_index[self.root.hash] = self.root
        self.all_node_hashes.add(self.root.hash)
        self.state.reset()
        self.apply_to_state(self.root)

    def _back_up_state(self):
        self.state.backup()

    def _restore_backed_up_state(self):
        self.state.restore_backup()

    @property
    def primary_chain(self):
        t = self.head
        primary_chain = []
        while not t.is_root:
            primary_chain = [t] + primary_chain
            t = self.get_block(t.links[0])
        return [t] + primary_chain  # root is not included in the above loop

    def _get_top_block(self):
        tb_hash = self._db.get_kv(TOP_BLOCK, int)
        if tb_hash is None:
            self._set_top_block(self.root)
            return self.root
        return self.block_index[tb_hash]

    def _set_top_block(self, top_block):
        return self._db.set_kv(TOP_BLOCK, top_block.hash)

    def seek_blocks(self, *block_hashes):
        s = set()
        for block_hash in block_hashes:
            if not self.has_block(block_hash) and block_hash not in self.currently_seeking:
                print('seeking', block_hash)
                s.add(block_hash)
        self.seeker.put(*s)

    def height_of_block(self, block_hash):
        return self.block_heights[block_hash]

    def has_block(self, block_hash):
        return block_hash in self.all_node_hashes

    def get_block(self, block_hash):
        return self.block_index[block_hash]

    def add_blocks(self, blocks):
        # todo : design some better sorting logic.
        # we should check if orphan chains match up with what we've added, if so add the orphan chain.
        rejects = []
        # todo: major bug - if blocks are added in the order [good, good, bad], say, and blocks 1 and 2 cause a reorg
        # then when block 3 causes an exception the state will revert but the head is still on block 2, which doesn't
        # match the state.
        self._back_up_state()
        total_works = [(b.total_work, b) for b in blocks]
        total_works.sort()
        try:
            for tw, block in total_works:
                r = self._add_block(block)
                if r is not None:
                    rejects.append(r)
            if rejects != blocks:
                self.add_blocks(rejects)
            else:
                print('rejects', rejects)
                for r in rejects:
                    self.orphans.put(r)
        except Exception as e:
            self._restore_backed_up_state()
            traceback.print_exc()

    def _add_block(self, block: SimpleBlock):
        """
        :param block: QuantaBlock instance
        :return: None on success, block if parent missing
        """
        print('_add_block', block.hash)
        if self.has_block(block.hash): return None
        if not block.acceptable_work: raise InvalidBlockException('Unacceptable work')
        if not all_true(self.has_block, block.links):
            print('Rejecting block: don\'t have all links')
            self.seek_blocks(*block.links)
            return block

        # success, lets add it
        self._update_metadata(block)
        block.set_block_index(self.block_index)
        if self.better_than_head(block):
            self.reorganize_to(block)
        self.all_node_hashes.add(block.hash)
        self.block_index[block.hash] = block
        print("Chain._add_block - processed", block.hash)
        orphaned_children = self.orphans.children_of(block.hash)
        if orphaned_children is not None:
            self.add_blocks(list(orphaned_children))
        return None

    def _set_height_metadata(self, block):
        height = self.block_heights[block.links[0]] + 1
        self.block_heights[block.hash] = height
        self.heights[height] = block.hash

    def _update_metadata(self, block):
        self._set_height_metadata(block)

    def reorganize_to(self, block):
        print('reorg from %064x\nto         %064x\nheight of  %d' % (self.head.hash, block.hash, self.block_heights[block.hash]))
        pivot = self.find_pivot(self.head, block)
        self.mass_unapply(self.order_from(pivot, self.head)[1:])
        self.mass_apply(self.order_from(pivot, block)[1:])
        print('Current State', self.state.full_state())
        self.head = block
        self._set_top_block(self.head)

    # Coin & State methods

    def get_next_state_hash(self, block):
        with self.state.lock:
            state_hash = self._get_next_state_hash_not_threadsafe(block)
        return state_hash

    def _get_next_state_hash_not_threadsafe(self, block):
        self._back_up_state()
        self._modify_state(block, 1)
        state_hash = self.state.hash
        self._restore_backed_up_state()
        return state_hash

    def valid_for_state(self, block):
        state_hash = self._get_next_state_hash_not_threadsafe(block)
        pp(self.state.full_state())
        pp(block.to_json())
        assert_equal(block.state_hash, state_hash)
        if block.tx is not None:
            assert self.state.get(block.tx.signature.pub_x) >= block.tx.total
        return True

    def apply_to_state(self, block):
        with self.state.lock:
            assert self.valid_for_state(block)
            self._modify_state(block, 1)

    def unapply_to_state(self, block):
        self._modify_state(block, -1)

    def _modify_state(self, block, direction):
        assert direction in [-1, 1]
        if block.tx is not None:
            self.state.modify_balance(block.tx.recipient, direction * block.tx.value)
            self.state.modify_balance(block.tx.signature.pub_x, -1 * direction * block.tx.value)
        self.state.modify_balance(block.coinbase, direction * block.coins_generated)
        print('Coinbase balance mod:', block.coinbase)

    def mass_unapply(self, path):
        for block in path[::-1]:
            self.unapply_to_state(block)

    def mass_apply(self, path):
        for block in path:
            self.apply_to_state(block)
            if block in self.orphans: self.orphans.remove(block)

    def better_than_head(self, block):
        return block.total_work > self.head.total_work

    def make_block_locator(self):
        locator = []

        h = self.block_heights[self.head.hash]
        print(h, self.head.hash)
        i = 0
        c = 0
        while h - c >= 0:
            locator.append(self.primary_chain[h - c].hash)
            c = 2**i
            i += 1

        return locator

    def _order_from_alpha(self, early_node, late_node):
        path = []
        print(early_node.hash)
        while early_node.hash != late_node.hash:
            if late_node.is_root:
                raise Exception("Root block encountered unexpectedly while ordering graph")
            path = [late_node] + path
            late_node = self.get_block(late_node.links[0])
            #print('new_late_node')
            #print(late_node.hash)
        return [early_node] + path

    def order_from(self, early_node: SimpleBlock, late_node: SimpleBlock):
        return self._order_from_alpha(early_node, late_node)

    def find_pivot(self, b1: SimpleBlock, b2: SimpleBlock):
        while b1.hash != b2.hash:
            if b1.total_work >= b2.total_work:
                b1 = self.get_block(b1.links[0])
            else:
                b2 = self.get_block(b2.links[0])
        return b1 if b1 == b2 else None
