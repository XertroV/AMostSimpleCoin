from WSSTT import Network

from structs import *


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

BLOCK_ANNOUNCE          = 'block_announce'
CHAIN_INFO              = 'chain_info'
CHAIN_INFO_PROVIDE      = 'chain_info_provide'
CHAIN_PRIMARY           = 'chain_primary'
CHAIN_PRIMARY_PROVIDE   = 'chain_primary_provide'
BLOCK_REQUEST           = 'block_request'
BLOCK_PROVIDE           = 'block_provide'
INV_REQUEST             = 'inv_request'
INV_PROVIDE             = 'inv_provide'

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
    block_locator = List.Definition(Hash.Definition())
    chunk_size = Integer8Bytes.Definition()
    chunk_n = Integer8Bytes.Definition()

class ChainPrimaryProvide(Encodium):
    hashes = List.Definition(Hash.Definition())
    chunk_n = Integer8Bytes.Definition()
    chunk_size = Integer8Bytes.Definition()


def set_message_handlers(chain, p2p: Network):

    @p2p.method(BlockAnnounce, BLOCK_ANNOUNCE, CHAIN_INFO)
    def block_announce(peer, announcement: BlockAnnounce):
        print('Got Block Ann')
        if not chain.has_block(announcement.block.hash):
            print ('Adding block')
            chain.add_blocks([announcement.block])
            p2p.broadcast(BLOCK_ANNOUNCE, announcement)

            if not chain.has_block(announcement.block.links[0]):
                return ChainInfoRequest()

    @p2p.method(BlockRequest, BLOCK_REQUEST, BLOCK_PROVIDE)
    def block_request(peer, request):
        print("Block Request")
        hashes = request.hashes[:500]  # return at most 500 blocks
        return BlockProvide(blocks=[chain.get_block(h) for h in hashes])

    @p2p.method(BlockProvide, BLOCK_PROVIDE)
    def block_provide(peer, provided):
        print("Block Provide")
        print(provided.blocks)
        chain.add_blocks(provided.blocks)

    @p2p.method(InvRequest, INV_REQUEST, INV_PROVIDE)
    def inv_request(peer, request):
        print("Inv Request")
        # todo : this needs to be O(1), bloom filters?
        return InvProvide(inv_list=[b.hash for b in chain.all_node_hashes])

    @p2p.method(InvProvide, INV_PROVIDE, BLOCK_REQUEST)
    def inv_provide(peer, provided):
        print("Inv Provide")
        to_request = []
        for h in provided.inv_list:
            if not chain.has_block(h):
                to_request.append(h)
        if len(to_request) > 0:
            return BlockRequest(hashes=to_request)

    @p2p.method(ChainInfoRequest, CHAIN_INFO, CHAIN_INFO_PROVIDE)
    def chain_info(peer, request):
        print("Chain Info")
        return ChainInfoProvide(top_block=chain.head.hash, total_work=chain.head.total_work)

    @p2p.method(ChainInfoProvide, CHAIN_INFO_PROVIDE, CHAIN_PRIMARY)
    def chain_info_provide(peer, provided):
        print("Chain Info Provide")
        if not chain.has_block(provided.top_block):
            size = 10000
            n = 0
            return ChainPrimaryRequest(block_locator=chain.make_block_locator(), chunk_size=size, chunk_n=n)

    @p2p.method(ChainPrimaryRequest, CHAIN_PRIMARY, CHAIN_PRIMARY_PROVIDE)
    def chain_primary(peer, request):
        print("Primary Chain")
        start = request.chunk_size * request.chunk_n

        lca = chain.root.hash
        for block_hash in request.block_locator:
            if chain.get_block(block_hash) in chain.primary_chain:
                lca = block_hash
            else:
                break

        print(chain.get_block(lca).to_json(), chain.get_block(lca).hash)
        print(chain.head.to_json(), chain.head.hash)
        print(request.block_locator)

        return ChainPrimaryProvide(
            hashes=[b.hash for b in chain.order_from(chain.get_block(lca), chain.head)[max(0, start - 10):start + request.chunk_size]],
            chunk_n=request.chunk_n,
            chunk_size=request.chunk_size)

    @p2p.method(ChainPrimaryProvide, CHAIN_PRIMARY_PROVIDE, CHAIN_PRIMARY)
    def chain_primary_provide(peer, provided):
        print("Primary Chain Provide")
        size = provided.chunk_size
        n = provided.chunk_n

        chain.seek_blocks(provided.hashes)

        if len(provided.hashes) >= size:
            return ChainPrimaryRequest(block_locator=chain.make_block_locator(), chunk_size=size, chunk_n=n+1)

