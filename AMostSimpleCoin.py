from hashlib import sha256
from copy import deepcopy
from queue import PriorityQueue, Empty
import sys
import time

import pycoin.ecdsa as ecdsa
from encodium import *
from WSSTT import Network
from WSSTT.structs import Peer

from helpers import *
from structs import *
from blockchain import Chain
from message_handlers import set_message_handlers
from control_loops import start_threads
from database import Database
from miner import Miner



port = int(sys.argv[sys.argv.index("-port") + 1]) if "-port" in sys.argv else 2281
extra_seed = sys.argv[sys.argv.index("-seed") + 1].split(":") if "-seed" in sys.argv else ('198.199.102.43', port)
db_num = int(sys.argv[sys.argv.index("-db") + 1] if "-db" in sys.argv else 0)


# Create chain

db = Database(db_num=db_num)

root_block = SimpleBlock(links=[], work_target=10**6, total_work=10**6, timestamp=1412226468, nonce=529437)
chain = Chain(root_block, db)

# Initialize P2P

seeds = [(extra_seed[0], int(extra_seed[1]))]
p2p = Network(seeds=seeds, address=('0.0.0.0', port), debug=True)

set_message_handlers(chain, p2p)

# inbuilt miner

miner = Miner(chain, p2p)

# Create root
if "-create_root" in sys.argv:
    root = SimpleBlock(links=[], work_target=10**6, total_work=10**6, timestamp=int(time.time()), nonce=miner._special_nonce)
    print(miner.mine_this_block(root).to_json())

# Go time!

try:
    start_threads(chain, p2p)
    if "-mine" in sys.argv: fire(miner.run)
    p2p.run()
finally:
    print('TERMINATING: DO NOT CLOSE')
    miner.stop()
    p2p.shutdown()