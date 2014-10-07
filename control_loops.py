import asyncio

from helpers import *
from message_handlers import *
from WSSTT.structs import Peer

# Chain crawler thing

@asyncio.coroutine
def watch_peer_top_blocks_loop(chain, p2p: Network):
    yield from asyncio.sleep(3)
    while not p2p.is_shutdown:
        p2p.broadcast(CHAIN_INFO, ChainInfoRequest())
        chain.seeker.follow_up()
        yield from asyncio.sleep(30)  # we don't want to piss people off if this is too frequent


def start_threads(chain, p2p):
    asyncio.async(watch_peer_top_blocks_loop(chain, p2p))
