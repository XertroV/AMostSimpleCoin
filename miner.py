import threading, time

from SSTT import Network

from blockchain import Chain
from structs import SimpleBlock
from helpers import fire, global_hash
from message_handlers import *

class Miner:

    def __init__(self, chain: Chain, p2p: Network, run_forever=False):
        self._chain = chain
        self._special_nonce = 1234567890
        self._run_forever = run_forever
        self._p2p = p2p
        self._running = False
        self._mining_thread = None

    def set_graph(self, graph):
        self._chain = graph

    def stop(self):
        self._stop = True
        self._mining_thread.join()

    def restart(self):
        self.stop()
        self.start(**self._mining_kwargs)

    def start(self, work_target=10**6):
        #self._mining_kwargs = {'coinbase': coinbase, 'tx': tx, 'target': target}
        self._mining_kwargs = {'work_target': work_target}
        candidate = SimpleBlock(links=[self._chain.head.hash], timestamp=int(time.time()), nonce=self._special_nonce, **self._mining_kwargs)

        self._stop = False
        self._mining_thread = fire(target=self._start_mining, args=[candidate])

    def _start_mining(self, candidate):

        # hack to replace a known special nonce, increase hash rate by modifying serialized blocks.
        m1, m2 = map(bytes,
                     candidate.to_json().split(
                         str(self._special_nonce).encode())
        )
        def serd_block_from_nonce(n):
            return m1 + str(n).encode() + m2

        nonce = 3056000  # normally start at 0
        target = candidate.target
        self._running = True
        while not self._stop:
            h = global_hash(serd_block_from_nonce(nonce))
            if h < target:
                candidate = SimpleBlock.from_json(serd_block_from_nonce(nonce).decode())
                break
            nonce += 1
            # if nonce % 100000 == 0: print(nonce)
        if candidate.acceptable_work:
            if self._chain: self._chain.add_blocks([candidate])
            if self._p2p: self._p2p.broadcast(BLOCK_ANNOUNCE, BlockAnnounce(block=candidate))
        self._running = False

        if self._run_forever and not self._stop:
            self.start(work_target=candidate.work_target)


if __name__ == '__main__':
    try:
        m = Miner(graph)
        while True:
            m.start(0)
            m._mining_thread.join()
    except KeyboardInterrupt:
        m.stop()
    print(graph.head.parent_hash)