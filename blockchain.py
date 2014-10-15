from copy import deepcopy
import traceback
import asyncio
import random

from WSSTT import Network

from structs import *
from helpers import *
from seeker import Seeker
from database import Database, RedisFlag, RedisHashMap, RedisSet, State, Orphanage, PrimaryChain

# TODO : figure out best where to hook DB in
TOP_BLOCK = 'top_block'

class Chain:
    def __init__(self, root: SimpleBlock, db: Database, p2p: Network):
        self._db = db
        self._p2p = p2p
        self.root = root

        self._state = State(self._db)

        self._orphans = Orphanage(self._db)
        self.current_node_hashes = RedisSet(db, 'all_nodes')
        self._block_index = RedisHashMap(db, 'block_index', int, SimpleBlock)
        self._block_heights = RedisHashMap(db, 'block_heights', int, int)
        self._heights = RedisHashMap(db, 'heights', int)
        self._initialized = RedisFlag(db, 'initialized')

        self.head = self._get_top_block()

        self._seeker = Seeker(self, self._p2p)  # format: (total_work, block_hash) - get early blocks first
        self.currently_seeking = set()

        # todo: temp till primary chain is done in redis so queries are quick
        self._primary_chain = PrimaryChain(self._db, 'primary_chain')

        if not self._initialized.is_true:
            self._first_initialize()
            self._initialized.set_true()

    def _first_initialize(self):
        self._heights[0] = self.root.hash
        self._block_heights[self.root.hash] = 0
        self._block_index[self.root.hash] = self.root
        self.current_node_hashes.add(self.root.hash)
        self._state.reset()
        self._apply_to_state(self.root)
        self._primary_chain.append_hashes([self.root.hash])

    def _back_up_state(self, backup_path):
        self._state.backup_to(backup_path)

    def _restore_backed_up_state(self, backup_path):
        self._state.restore_backup_from(backup_path)

    @property
    def primary_chain(self):
        return self._primary_chain.get_all()

    def _get_top_block(self):
        tb_hash = self._db.get_kv(TOP_BLOCK, int)
        if tb_hash is None:
            self._set_top_block(self.root)
            return self.root
        return self._block_index[tb_hash]

    def _set_top_block(self, top_block):
        return self._db.set_kv(TOP_BLOCK, top_block.hash)

    def seek_blocks(self, block_hashes):
        self._seeker.put(*[h for h in block_hashes if not self.has_block(h)])

    def seek_blocks(self, block_hashes):
        self._seeker.put(*[h for h in block_hashes if not self.has_block(h)])

    def seek_blocks_with_total_work(self, pairs):
        self._seeker.put_with_work(*pairs)

    def height_of_block(self, block_hash):
        return self._block_heights[block_hash]

    def has_block(self, block_hash):
        return block_hash in self.current_node_hashes or self._orphans.contains_block_hash(block_hash)

    def contains_block(self, block_hash):
        return block_hash in self.current_node_hashes

    def get_block(self, block_hash):
        return self._block_index[block_hash]

    def add_blocks(self, blocks):
        # todo : design some better sorting logic.
        # we should check if orphan chains match up with what we've added, if so add the orphan chain.
        rejects = []
        # todo: major bug - if blocks are added in the order [good, good, bad], say, and blocks 1 and 2 cause a reorg
        # then when block 3 causes an exception the state will revert but the head is still on block 2, which doesn't
        # match the state.  - I think this is fixed now
        some_path = str(random.randint(1000,1000000))
        self._back_up_state(some_path)

        total_works = [(b.total_work, b) for b in blocks]
        total_works.sort()

        most_recent_block = None

        try:
            while True:
                if len(total_works) == 0:
                    break
                tw, block = total_works.pop(0)
                most_recent_block = block
                r = self._add_block(block)
                if isinstance(r, list):
                    total_works.extend([(b.total_work, b) for b in r])
                elif isinstance(r, Encodium):
                    rejects.append(r)
            print('rejects', rejects)
            for r in rejects:
                self._orphans.add(r)
        except Exception as e:
            self._restore_backed_up_state(some_path)
            traceback.print_exc()
            print('EXCEPTION CAPTURED WHILE ADDING BLOCK', most_recent_block.to_json())

    def _add_block(self, block: SimpleBlock):
        """
        :param block: QuantaBlock instance
        :return: None on success, block if parent missing
        """
        print('_add_block', block.hash)
        if block.hash in self.current_node_hashes: return None
        if not block.acceptable_work: raise InvalidBlockException('Unacceptable work')
        if not all_true(self.contains_block, block.links):
            print('Rejecting block: don\'t have all links')
            # don't just look for children, get a primary chain
            self.seek_blocks({i for i in block.links if not self._orphans.contains_block_hash(i)})
            return block

        # success, lets add it
        self._update_metadata(block)
        block.set_block_index(self._block_index)
        if self.better_than_head(block):
            print('COINBASE _add_blk', block.coinbase)
            self._reorganize_to(block)
        self.current_node_hashes.add(block.hash)
        self._block_index[block.hash] = block
        print("Chain._add_block - processed", block.hash)
        orphaned_children = self._orphans.children_of(block.hash)
        self._orphans.remove(block)
        if len(orphaned_children) > 0:
            print([self._orphans.get(h) for h in orphaned_children])
            return [self._orphans.get(h) for h in orphaned_children]
        self._orphans.remove(block)
        return None

    def _set_height_metadata(self, block):
        height = self._block_heights[block.links[0]] + 1
        self._block_heights[block.hash] = height
        self._heights[height] = block.hash

    def _update_metadata(self, block):
        self._set_height_metadata(block)

    def _mass_primary_chain_apply(self, path):
        self._primary_chain.append_hashes([b.hash for b in path])

    def _mass_primary_chain_unapply(self, path):
        self._primary_chain.remove_hashes([b.hash for b in path])

    def _reorganize_to(self, block):
        print('reorg from %064x\nto         %064x\nheight of  %d' % (self.head.hash, block.hash, self._block_heights[block.hash]))
        pivot = self.find_pivot(self.head, block)
        unapply_path = self.order_from(pivot, self.head)
        self._mass_unapply(unapply_path)
        self._mass_primary_chain_unapply(unapply_path)
        print('COINBASE _re_org_', block.coinbase)
        apply_path = self.order_from(pivot, block)
        self._mass_apply(apply_path)
        self._mass_primary_chain_apply(apply_path)
        print('Current State')
        pp(self._state.full_state())
        self.head = block
        self._set_top_block(self.head)

    # Coin & State methods

    def get_next_state_hash(self, block):
        with self._state.lock:
            state_hash = self._get_next_state_hash_not_threadsafe(block)
        return state_hash

    def _get_next_state_hash_not_threadsafe(self, block):
        temp_path = str(random.randint(1000,1000000))
        self._back_up_state(temp_path)
        self._modify_state(block, 1)
        state_hash = self._state.hash
        self._restore_backed_up_state(temp_path)
        return state_hash

    def _valid_for_state(self, block):
        state_hash = self._get_next_state_hash_not_threadsafe(block)
        assert_equal(block.state_hash, state_hash)
        if block.tx is not None:
            assert self._state.get(block.tx.signature.pub_x) >= block.tx.total
        return True

    def _apply_to_state(self, block):
        with self._state.lock:
            print('COINBASE _aply_st', block.coinbase)
            assert self._valid_for_state(block)
            assert self._valid_for_state(block)
            self._modify_state(block, 1)

    def _unapply_to_state(self, block):
        self._modify_state(block, -1)

    def _modify_state(self, block, direction):
        assert direction in [-1, 1]
        if block.tx is not None:
            self._state.modify_balance(block.tx.recipient, direction * block.tx.value)
            self._state.modify_balance(block.tx.signature.pub_x, -1 * direction * block.tx.value)
        self._state.modify_balance(block.coinbase, direction * block.coins_generated)

    def _mass_unapply(self, path):
        for block in path[::-1]:
            self._unapply_to_state(block)

    def _mass_apply(self, path):
        print(path)
        for block in path:
            print('COINBASE _ms_aply', block.coinbase)
            self._apply_to_state(block)
            if block in self._orphans:
                self._orphans.remove(block)

    def better_than_head(self, block):
        return block.total_work > self.head.total_work

    def make_block_locator(self):
        locator = []

        h = self._block_heights[self.head.hash]
        print(h, self.head.hash)
        i = 0
        c = 0
        while h - c >= 0:
            locator.append(self.primary_chain[h - c])
            c = 2**i
            i += 1

        return locator

    def _order_from_alpha(self, early_node, late_node):
        path = []
        print(early_node.hash)
        while early_node.hash != late_node.hash:
            path = [late_node] + path
            if late_node.is_root:
                if early_node.is_root:
                    return path
                raise Exception("Root block encountered unexpectedly while ordering graph")
            late_node = self.get_block(late_node.links[0])
            #print('new_late_node')
            #print(late_node.hash)
        return path

    def _order_from_beta(self, early_node, late_node):
        pass

    def order_from(self, early_node: SimpleBlock, late_node: SimpleBlock):
        return self._order_from_alpha(early_node, late_node)

    def find_pivot(self, b1: SimpleBlock, b2: SimpleBlock):
        while b1.hash != b2.hash:
            if b1.total_work >= b2.total_work:
                b1 = self.get_block(b1.links[0])
            else:
                b2 = self.get_block(b2.links[0])
        return b1 if b1 == b2 else None
