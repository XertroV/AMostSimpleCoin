
from SSTT import Network

from structs import *
from blockchain import Chain


""" Message Protocol:

Node-info is provided with each request.

CHAIN_INFO (-> top_hash: Hash, total_work: Integer) requests details of the current chain. The top block's hash and
total work are reported. Total work is sometimes referred to as sigmadiff, or 'total difficulty' by the Ethereum crew.

CHAIN_PRIMARY (chunk_size: int, n: int -> [h: Hash, ..]) requests the segment of the primary chain (in hashes alone) of
chunk_size and the nth such chunk. Some (1, or 10, or w/e) hashes before this chunk should be included to help
sync overlapping sections of the main chain. Less hashes will be provided than chunk_size if it is appropriate.

BLOCK_REQUEST ([h: Hash, ..] -> [b: Block, ..])  A list of hashes is provided; a list of blocks is returned. The
absence of a block belonging to a hash (in the reply) is not evidence the block does not exist, a node may return
fewer blocks than the provided hashes requests. If a block is not found no special indication is given. (A node can
check the inventory if they wish.)

INV_REQUEST (-> [h: Hash, ..]) Return hashes of items in inventory.
 todo: segregate txs, blocks, etc
 todo: use bloom filters instead of list


BLOCK_ANNOUNCE (b: Block ->) Push a block to a node.
"""

BLOCK_ANNOUNCE = 'block_announce'
CHAIN_INFO = 'chain_info'
CHAIN_PRIMARY = 'chain_primary'
BLOCK_REQUEST = 'block_request'
INV_REQUEST = 'inv_request'

# Message Containers

class BlockAnnounce(Encodium):
    block = SimpleBlock.Definition()

class BlockRequest(Encodium):
    hashes = List.Definition(Hash.Definition())  # blocks we're requesting

class BlockProvide(Encodium):
    blocks = List.Definition(SimpleBlock.Definition())

class InvRequest(Encodium):
    pass

class InvProvide(Encodium):
    inv_list = List.Definition(Hash.Definition())

class ChainInfoRequest(Encodium):
    pass

class ChainInfoProvide(Encodium):
    top_block = Hash.Definition()
    total_work = Integer32Bytes.Definition()

class ChainPrimaryRequest(Encodium):
    block_locator = List.Definition(SimpleBlock.Definition())
    chunk_size = Integer8Bytes.Definition()
    chunk_n = Integer8Bytes.Definition()

class ChainPrimaryProvide(Encodium):
    hashes = List.Definition(Hash.Definition())
    chunk_n = Integer8Bytes.Definition()
    chunk_size = Integer8Bytes.Definition()


def set_message_handlers(p2p: Network, chain: Chain):

    @p2p.method(BlockAnnounce)
    def block_announce(announcement: BlockAnnounce):
        if not chain.has_block(announcement.block.hash):
            chain.add_blocks([announcement.block])
            p2p.broadcast(BLOCK_ANNOUNCE, announcement)

    @p2p.method(BlockRequest)
    def block_request(request):
        hashes = request.hashes[:500]  # return at most 500 blocks
        return BlockProvide(blocks=[chain.get_block(h) for h in hashes])

    @p2p.method(InvRequest)
    def inv_request(request):
        # todo : this needs to be O(1), bloom filters?
        return InvProvide(inv_list=[b.hash for b in chain.all_nodes])

    @p2p.method(ChainInfoRequest)
    def chain_info(request):
        return ChainInfoProvide(top_block=chain.head.hash, total_work=chain.head.sigma_diff)

    @p2p.method(ChainPrimaryRequest)
    def chain_primary(request):
        start = request.chunk_size * request.chunk_n

        lca = None
        for block_hash in request.block_locator:
            if chain.has_block(block_hash):
                lca = block_hash
            else:
                break

        if lca is None:
            lca = chain.root.hash

        return ChainPrimaryProvide(
            blocks=chain.order_from(chain.get_block(lca), chain.head)[max(0, start - 10):start + request.chunk_size],
            chunk_n=request.chunk_n,
            chunk_size=request.chunk_size)