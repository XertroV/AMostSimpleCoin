import threading, time, random, asyncio

from WSSTT import Network

from blockchain import Chain
from structs import SimpleBlock
from helpers import fire, global_hash
from message_handlers import *

class Miner:

    def __init__(self, chain: Chain=None, p2p: Network=None, coinbase=PUB_KEY_X_FOR_KNOWN_SE, run_forever=True):
        self._chain = chain
        self._special_nonce = 1234567890
        self._run_forever = run_forever
        self._p2p = p2p
        self._running = False
        self._mining_thread = None
        self._stop = False
        self._coinbase = coinbase
        self._mine_fast = False

    def mine_fast(self):
        self._mine_fast = True

    def stop(self):
        self._stop = True
        if self._mining_thread: self._mining_thread.join()

    def restart(self):
        self.stop()
        self.start()

    def run(self, work_target=10**6):
        nice_sleep(self._p2p, 3)  # warm up
        while not self._stop and not self._p2p.is_shutdown:
            self.start()
            if self._mine_fast:
                nice_sleep(self._p2p, 0.5)
            else:
                nice_sleep(self._p2p, random.randint(60, 120))

    def start(self, work_target=10**5+1):
        chain_head = self._chain.head
        candidate = SimpleBlock(links=[chain_head.hash], timestamp=int(time.time()), nonce=self._special_nonce,
                                work_target=work_target, total_work=chain_head.total_work + work_target,
                                coinbase=self._coinbase)

        self._stop = False
        self._mining_thread = fire(target=self._start_mining, args=[candidate])
        self._mining_thread.join()

    def _start_mining(self, candidate):

        # todo: edge case where str(nonce) gains characters, altering the storage fee, causing the state_hash to change...

        while candidate.state_hash != self._chain.get_next_state_hash(candidate):
            candidate.state_hash = self._chain.get_next_state_hash(candidate)
        candidate = self.mine_this_block(candidate)

        if candidate is not None:
            # try not adding it directly and just broadcasting
            if self._p2p:
                print('Announcing Block')
                self._p2p.broadcast(BLOCK_ANNOUNCE, BlockAnnounce(block=candidate))
        self._running = False

    def mine_this_block(self, candidate: SimpleBlock):

        # hack to replace a known special nonce, increase hash rate by modifying serialized blocks.
        m1, m2 = map(lambda x : x.encode(), candidate.to_json().split(str(self._special_nonce)))
        def serialized_block_from_nonce(n):
            return m1 + str(n).encode() + m2

        nonce = self._special_nonce  # set large to avoid edge case presented
        work_target = candidate.work_target
        hash_target = work_target_to_hash_target(work_target)
        self._running = True
        while not self._stop and not self._p2p.is_shutdown:
            h = global_hash(serialized_block_from_nonce(nonce))
            if h < hash_target:
                candidate.nonce = nonce
                assert candidate.acceptable_work
                break
            nonce += 1
            # if nonce % 100000 == 0: print(nonce)

        if self._stop or self._p2p.is_shutdown:
            return
        return candidate







