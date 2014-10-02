import threading, time

from SSTT import Network

from blockchain import Chain
from structs import SimpleBlock
from helpers import fire, global_hash
from message_handlers import *

class Miner:

    def __init__(self, chain: Chain=None, p2p: Network=None, run_forever=False):
        self._chain = chain
        self._special_nonce = 1234567890
        self._run_forever = run_forever
        self._p2p = p2p
        self._running = False
        self._mining_thread = None
        self._stop = False

    def set_graph(self, graph):
        self._chain = graph

    def stop(self):
        self._stop = True
        self._mining_thread.join()

    def restart(self):
        self.stop()
        self.start()

    def run(self, work_target=10**6):
        while True:
            self.start()
            self._mining_thread.join()

    def start(self, work_target=10**6):
        candidate = SimpleBlock(links=[self._chain.head.hash], timestamp=int(time.time()), nonce=self._special_nonce, work_target=work_target, total_work=self._chain.head.total_work + work_target)

        self._stop = False
        self._mining_thread = fire(target=self._start_mining, args=[candidate])
        self._mining_thread.join()

    def mine_this_block(self, candidate: SimpleBlock):

        # hack to replace a known special nonce, increase hash rate by modifying serialized blocks.
        m1, m2 = map(lambda x : x.encode(), candidate.to_json().split(str(self._special_nonce)))
        def serialized_block_from_nonce(n):
            return m1 + str(n).encode() + m2

        nonce = 0
        work_target = candidate.work_target
        hash_target = work_target_to_hash_target(work_target)
        self._running = True
        while not self._stop:
            h = global_hash(serialized_block_from_nonce(nonce))
            if h < hash_target:
                candidate.nonce = nonce
                assert candidate.acceptable_work
                break
            nonce += 1
            # if nonce % 100000 == 0: print(nonce)

        return candidate

    def _start_mining(self, candidate):

        candidate = self.mine_this_block(candidate)
        if self._chain: self._chain.add_blocks([candidate])
        if self._p2p: self._p2p.broadcast(BLOCK_ANNOUNCE, BlockAnnounce(block=candidate))
        self._running = False

        if self._run_forever and not self._stop:
            self.start(work_target=candidate.work_target)