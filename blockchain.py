from copy import deepcopy

from structs import *
from helpers import *
from database import Database

# TODO : figure out best where to hook DB in

class Chain:
    def __init__(self, root: SimpleBlock):
        self.root = root
        self.head = self.root
        self.orphans = Orphanage()
        self.all_nodes = {self.root}
        self.state = State()
        self.block_index = {self.root.hash: self.root}
        #self.apply_to_state(self.root)

        self.blocks_to_seek = PriorityQueue()  # format: (total_work, block_hash) - get early blocks first


    def _back_up_state(self):
        self.backup_state = deepcopy(self.state)

    def _restore_backed_up_state(self):
        self.state = self.backup_state

    def seek_block(self, total_work, block_hash):
        if not self.has_block(block_hash):
            self.blocks_to_seek.put((total_work, block_hash))

    def has_block(self, block_hash):
        return block_hash in self.block_index

    def get_block(self, block_hash):
        return self.block_index[block_hash]

    def add_blocks(self, blocks):
        print(blocks)
        # todo : design some better sorting logic.
        # we should check if orphan chains match up with what we've added, if so add the orphan chain.
        rejects = []
        self._back_up_state()
        try:
            for block in blocks:
                r = self._add_block(block)
                if r is not None:
                    rejects.append(r)
            if rejects != blocks:
                self.add_blocks(rejects)
            else:
                for r in rejects:
                    self.orphans.put(r)
        except Exception as e:
            self._restore_backed_up_state()

    def _add_block(self, block: SimpleBlock):
        """
        :param block: QuantaBlock instance
        :return: None on success, block if parent missing
        """
        if self.has_block(block.hash): return None
        if not block.acceptable_work: raise InvalidBlockException('Unacceptable work')
        if not all_true(self.has_block, block.links):
            return block
        block.set_linked_blocks([self.block_index[link] for link in block.links])
        if self.better_than_head(block):
            self.reorganize_to(block)
        self.all_nodes.add(block)
        self.block_index[block.hash] = block
        print("Chain._add_block - processed", block.hash)
        return None

    def reorganize_to(self, block):
        print('reorg from %064x\nto         %064x\n' % (self.head.hash, block.hash))
        pivot = self.find_pivot(self.head, block)
        self.mass_unapply(Chain.order_from(pivot, self.head)[1:])
        self.mass_apply(Chain.order_from(pivot, block)[1:])
        self.head = block

    # Coin & State methods

    def valid_for_state(self, block):
        if block.tx == None: return True
        return self.state.get(block.tx.signature.pub_x) >= block.tx.value

    def apply_to_state(self, block):
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

        h = self.head.height
        i = 0
        c = 0
        while h - c >= 0:
            locator.append(self.head.primary_chain[h - c])
            c = 2**i
            i += 1

        return locator

    # Static Methods

    '''
    @staticmethod
    def order_from(early_node, late_node, carry=None):
        carry = [] if carry is None else carry
        if early_node == late_node:
            return [late_node]
        if late_node.parent_hash == 0:
            raise Exception('Root block encountered unexpectedly while ordering graph')
        main_path = exclude_from(Graph.order_from(early_node, late_node.parent), carry)
        aux_path = exclude_from(Graph.order_from(early_node, late_node.uncle), carry + main_path) if late_node.uncle is not None else []
        return main_path + aux_path + [late_node]
    '''

    @staticmethod
    def _order_from_alpha(early_node, late_node, carry=None, pivot_check=True):
        # TODO: may need to check this alg in boundary cases to ensure
        carry = [] if carry is None else carry
        if early_node == late_node:
            return [late_node]
        if late_node.is_root:
            raise Exception("Root block encountered unexpectedly while ordering graph")
        if pivot_check:
            assert Chain.find_pivot(early_node, late_node) == early_node
        path = []
        for linked_block in late_node.linked_blocks:
            path.extend(exclude_from(Chain._order_from_alpha(early_node, linked_block), carry + path))
        return path + [late_node]

    '''@staticmethod
    def _order_from_beta(early_node, late_node, already_ordered=None):
        already_ordered = set() if already_ordered is None else already_ordered
        path = []
        for linked_block in late_node.linked_blocks:
            if linked_block not in already_ordered:
                order = Graph._order_from_beta(early_node, linked_block, already_ordered)
                path.extend(order)
                already_ordered.union(set(order))
        return path + [late_node]'''

    @staticmethod
    def order_from(early_node: SimpleBlock, late_node: SimpleBlock):
        return Chain._order_from_alpha(early_node, late_node)

    @staticmethod
    def find_pivot(b1: SimpleBlock, b2: SimpleBlock):
        # conjecture: rewinding back to the lowest common ancestor in the primary chain is sufficient (and necessary?)
        # the primary chain is constructed by taking the first link from a block, which is of the highest priority.
        while b1 != b2 and not b1.is_root and not b2.is_root:
            if b1.total_work >= b2.total_work:
                b1 = b1.linked_blocks[0]
            else:
                b2 = b2.linked_blocks[0]
        return b1 if b1 == b2 else None

    def dump_to_db(self, db: Database):  # infrequent use intended
        offset = db.create_new_index_of_chain(self)  # enumerates block hashes in order of execution
        db.dump_chain(self)  # map of block_hash to block.to_json()
        # todo: decide if a verification is needed
        # db.verify_chain_at_index(self, offset)  # goes through and checks the above
        db.purge(offset-1)  # remove the last copy - this is meant to be done infrequently

    def load_from_db(self, db):
        index = db.get_latest_index()
        print(index)
        while len(index) > 0:
            self.add_blocks(db.get_blocks(index[:500]))
            index = index[500:]

