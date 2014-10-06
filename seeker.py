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

    def any_to_follow_up(self):
        if self._follow_up.empty():
            return False
        peek = self._follow_up.get_nowait()
        self._follow_up.put_nowait(peek)
        print('peek, should not be generator', peek)
        if current_time() - peek[0] > 10:
            return True
        return False

    def follow_up(self):
        if not self.any_to_follow_up():
            return
        hashes = set()
        while self.any_to_follow_up():
            ts, h = self._follow_up.get_nowait()
            hashes.add(h)

        self._chain.seek_blocks(hashes)

    def put(self, *block_hashes):

        blocks_at_a_time = 100

        print("Seeker.put", block_hashes)
        s = set(block_hashes)
        s = s.difference(self._chain.currently_seeking)
        if len(s) > 0:
            s = list(s)
            for i in range(len(block_hashes) // blocks_at_a_time + 1):
                chunk = s[i * blocks_at_a_time : (i + 1) * blocks_at_a_time]
                asyncio.async(self._p2p.farm_message(BLOCK_REQUEST, BlockRequest(hashes=chunk)))
                for h in chunk:
                    self._chain.currently_seeking.add(h)
                (yield from asyncio.sleep(3))


        timestamp = time.time()

        self.follow_up()

        for hash in s:
            self._follow_up.put((timestamp, hash))