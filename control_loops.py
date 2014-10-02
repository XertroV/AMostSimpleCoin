from helpers import *
from message_handlers import *
from SSTT.structs import Peer

# Chain crawler thing

def watch_blocks_to_seek(chain: Chain, p2p: Network):
    nice_sleep(p2p, 3) # warm up

    not_found_count = {}

    while not p2p._shutdown:
        try:
            seek_blocks = get_n_from_pq_and_block(500, chain.blocks_to_seek, timeout=0.1)
        except Empty:
            continue
        blocks_provided = p2p.request_an_obj_from_hive(BlockProvide, BLOCK_REQUEST, BlockRequest(hashes=[h for tw, h in seek_blocks]))

        # remove blocks we've been given
        for block in blocks_provided.blocks:
            pair = (block.total_work, block.hash)
            if pair in seek_blocks:
                seek_blocks.remove(pair)

        # add blocks to the chain, seek what we didn't find (if it's not spam)
        chain.add_blocks(blocks_provided.blocks)
        for pair in seek_blocks:
            if pair not in not_found_count:
                not_found_count[pair] = 0
            not_found_count[pair] += 1
            if not_found_count[pair] > 10:
                # don't continue to seek it
                continue
            chain.blocks_to_seek.put(pair)


def watch_peer_top_blocks_loop(chain: Chain, p2p: Network):
    ''' Loop to watch top blocks and if an unknown block is seen it should be sought.

    General logic:
    Look for unknown top_blocks
    If any are found, send block_locators and ChainPrimaryRequests,
    If any are recieved, add blocks to be sought.
    Sleep

    :param chain: the Chain object
    :param p2p: an SSTT.Network object
    :return: None
    '''
    nice_sleep(p2p, 3)

    def any_new_blocks():
        chain_infos = p2p.broadcast_with_response(ChainInfoProvide, CHAIN_INFO, Encodium())
        for chain_info in chain_infos:
            assert isinstance(chain_info, ChainInfoProvide)
            if chain_info.top_block not in chain.all_nodes:
                return True
        return False

    def collect_primary_chain_from_peer(peer: Peer, block_locator):
        n = 0
        size = 10000
        primary_chain = []
        none_counter = 0
        while none_counter < 20:
            chunk = p2p.request_an_obj_from_peer(ChainPrimaryProvide, peer, CHAIN_PRIMARY, ChainPrimaryRequest(block_locator=block_locator, chunk_size=size, chunk_n=n))
            if chunk == None:
                none_counter += 1
                continue
            primary_chain.extend(chunk.blocks)
            break  # todo: this is temp, but will mean the loop terminates early; we'll still sync, though
            #if len(chunk) < size:
            #    break
        return primary_chain

    def add_primary_chain_to_results(peer, results, block_locator):
        ''' seek primary chain from peer add to results.
        '''
        results.append(collect_primary_chain_from_peer(peer, block_locator))

    while not p2p._shutdown:
        if any_new_blocks():
            # send block locators to each peer
            with p2p.active_peers_lock:
                peers = p2p.all_peers()
            block_locator = chain.make_block_locator()
            results = []
            threads = [fire(add_primary_chain_to_results, args=(p, results, block_locator)) for p in peers]
            wait_for_all_threads_to_finish(threads)
            counter = 0  # the counter ensures hashes added will be sought in the order submitted
            for hashes in results:
                counter += 1
                for n, block_hash in enumerate(hashes):
                    chain.seek_block(n + counter * 1000000, block_hash)  # todo: maybe a better priority system than just enumeration

        nice_sleep(p2p, 30)  # we don't want to piss people off if this is too frequent


def start_threads(chain, p2p):
    fire(watch_blocks_to_seek, args=(chain, p2p))
    fire(watch_peer_top_blocks_loop, args=(chain, p2p))
