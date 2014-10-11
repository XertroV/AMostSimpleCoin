import asyncio, time

from message_handlers import BLOCK_REQUEST, BlockRequest

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
        self._follow_up_at_most_at_once = 100

        asyncio.get_event_loop().call_soon(self.follow_up)

    def any_to_follow_up(self):
        if self._follow_up.empty():
            return False
        peek = self._follow_up.get_nowait()
        self._follow_up.put_nowait(peek)
        if current_time() - peek[0] > self._time_to_wait_before_follow_up:
            return True
        return False

    def follow_up(self):
        if not self.any_to_follow_up():
            return
        still_to_seek = []  # list of block hashes
        done = set()  # block hashes
        while self.any_to_follow_up() and len(still_to_seek) < self._follow_up_at_most_at_once:
            ts, h = self._follow_up.get_nowait()
            if not self._chain.has_block(h):
                still_to_seek.append(h)
                self._follow_up.put_nowait((ts, h))
            else:
                done.add(h)

        self._chain.currently_seeking = self._chain.currently_seeking.difference(done)
        self.farm_seek(still_to_seek)

        asyncio.get_event_loop().call_later(self._time_to_wait_before_follow_up // 4, self.follow_up)

    def put(self, *block_hashes):
        s = [h for h in block_hashes if h not in self._chain.currently_seeking]
        self.farm_seek(s)

        for hash in s:
            self._follow_up.put_nowait((time.time(), hash))

    def farm_seek(self, block_hashes):
        if len(block_hashes) > 0:
            asyncio.async(self._p2p.farm_message(BLOCK_REQUEST, BlockRequest(hashes=block_hashes)))
            for h in block_hashes:
                self._chain.currently_seeking.add(h)
