import asyncio, time

from message_handlers import BLOCK_REQUEST, BlockRequest
from helpers import MAX_32_BYTE_INT

def current_time():
    return int(time.time())

class Seeker:
    """ The Seeker will actively request and track block hashes.
    """
    def __init__(self, chain, p2p):
        self._chain = chain
        self._p2p = p2p
        self._follow_up = asyncio.PriorityQueue()
        self._time_to_wait_before_follow_up = 2
        # todo: write an alg that changes this variable depending on how many good blocks we get back to find
        # a practical maximum
        self._follow_up_at_most_at_once = 50

        asyncio.get_event_loop().call_soon(self.follow_up)

    def any_to_follow_up(self):
        if self._follow_up.empty():
            return False
        peek = self._follow_up.get_nowait()
        self._follow_up.put_nowait(peek)
        print(peek)
        if current_time() - peek[1] > self._time_to_wait_before_follow_up:
            return True
        return False

    def follow_up(self):
        if not self.any_to_follow_up():
            return
        still_to_seek = []  # list of block hashes
        done = set()  # block hashes
        put_soon = []
        while self.any_to_follow_up() and len(still_to_seek) < self._follow_up_at_most_at_once:
            height, ts, h = self._follow_up.get_nowait()
            if not self._chain.has_block(h):
                still_to_seek.append(h)
                put_soon.append((height, time.time(), h))  # need to use current time, not old time
            else:
                done.add(h)

        for t in put_soon:
            self._put_nowait(*t)

        self._chain.currently_seeking = self._chain.currently_seeking.difference(done)
        self.farm_seek(still_to_seek)

        asyncio.get_event_loop().call_later(self._time_to_wait_before_follow_up + 1, self.follow_up)

    def put(self, *block_hashes):
        s = [h for h in block_hashes if h not in self._chain.currently_seeking]
        self.farm_seek(s)

        for hash in s:
            self._put_nowait(MAX_32_BYTE_INT, time.time(), hash)  # really big height as first in tuple

    def put_with_work(self, *pairs):
        s = [h for w, h in pairs if h not in self._chain.currently_seeking]
        self.farm_seek(s)

        for work, hash in pairs:
            self._put_nowait(work, time.time(), hash)

    def _put_nowait(self, total_work, timestamp, hash):
        self._follow_up.put_nowait((total_work, timestamp, hash))

    def farm_seek(self, block_hashes):
        if len(block_hashes) > 0:
            asyncio.async(self._p2p.farm_message(BLOCK_REQUEST, BlockRequest(hashes=block_hashes)))
            for h in block_hashes:
                self._chain.currently_seeking.add(h)
