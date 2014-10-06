import asyncio, time

from message_handlers import BLOCK_REQUEST, BlockRequest

class Seeker:
    """ The Seeker will actively request and track block hashes.
    """
    def __init__(self, chain, p2p):
        self._chain = chain
        self._p2p = p2p
        self._follow_up = asyncio.PriorityQueue()

    def put(self, *block_hashes):
        print("Seeker.put")
        s = set(block_hashes)
        s = s.difference(self._chain.currently_seeking)
        if len(s) > 0:
            asyncio.async(self._p2p.farm_message(BLOCK_REQUEST, BlockRequest(hashes=list(s))))
            for h in s:
                self._chain.currently_seeking.add(h)

        timestamp = time.time()

        if not self._follow_up.empty():
            hashes = set()
            while not self._follow_up.empty():
                ts, h = self._follow_up.get_nowait()
                if timestamp - ts < 10:
                    self._follow_up.put_nowait((ts, h))
                    break
                hashes.add(h)
            found = {h for h in hashes if self._chain.has_block(h)}
            self._chain.currently_seeking = self._chain.currently_seeking.difference(found)
            hashes = hashes.difference(found)
            self._chain.seek_blocks(*hashes)
            s = s.difference(found)
            s = s.difference(hashes)

        for hash in s:
            self._follow_up.put((timestamp, hash))