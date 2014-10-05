import asyncio

from helpers import *
from message_handlers import *
from WSSTT.structs import Peer

# Chain crawler thing

@asyncio.coroutine
def watch_blocks_to_seek(chain: Chain, p2p: Network):
    nice_sleep(p2p, 3) # warm up

    check_again = PriorityQueue()

    while not p2p.is_shutdown:
        try:
            seek_blocks = get_n_from_pq_and_block(500, chain.blocks_to_seek, timeout=0.1)
        except Empty:
            continue
        print("seeking blocks", seek_blocks)
        try:
            p2p.farm_message(BLOCK_REQUEST, BlockRequest(hashes=[h for _, h in seek_blocks]))
        except Exception as e:
            raise

        timestamp = time.time()
        for hash in seek_blocks:
            check_again.put((timestamp, hash))

        try:
            checking = (t, (n, h)) = check_again.get()
            while timestamp - checking[0] > 10:
                if not chain.has_block(checking[1][1]):
                    chain.seek_block(*checking[1])
                checking = check_again.get()
            check_again.put(checking)  # put the last one back because it's not ready yet
        except Empty:
            pass



def watch_peer_top_blocks_loop(chain: Chain, p2p: Network):
    nice_sleep(p2p, 3)
    while not p2p.is_shutdown:
        p2p.broadcast(CHAIN_INFO, ChainInfoRequest())
        nice_sleep(p2p, 30)  # we don't want to piss people off if this is too frequent


def start_threads(chain, p2p):
    fire(watch_blocks_to_seek, args=(chain, p2p))
    fire(watch_peer_top_blocks_loop, args=(chain, p2p))
