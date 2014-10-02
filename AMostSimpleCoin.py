from hashlib import sha256
from copy import deepcopy
from queue import PriorityQueue, Empty
import sys
import time

from flask import request

import pycoin.ecdsa as ecdsa
from encodium import *
from SSTT import Network
from SSTT.utils import fire, nice_sleep
from SSTT.structs import Peer

from helpers import *
from structs import *
from blockchain import Chain
from message_handlers import set_message_handlers
from control_loops import start_threads
from database import Database
from miner import Miner


# Create chain

db = Database()

root_block = SimpleBlock(links=[], work_target=(256 ** 3), total_work=(256 ** 2), timestamp=0, nonce=1901667)
chain = Chain(root_block)
chain.load_from_db(db)

# Initialize P2P

port = 2281
seeds = [('198.199.102.43', port-1), ('127.0.0.1', port)]
p2p = Network(seeds=seeds, address=('0.0.0.0', port), debug=True)

set_message_handlers(chain, p2p)

# Create root
if "-create_root" in sys.argv:
    m = Miner(chain, p2p)
    root = SimpleBlock(links=[], work_target=10**6, total_work=0, timestamp=int(time.time()), nonce=m._special_nonce)
    print(m._miner_this_block(root))

# Go time!

try:
    start_threads(chain, p2p)
    p2p.run()
finally:
    p2p.shutdown()
    print('TERMINATING: DO NOT CLOSE')
    chain.dump_to_db(db)