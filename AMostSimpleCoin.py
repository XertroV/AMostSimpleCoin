import sys, logging
import time

from encodium import *
from WSSTT import Network
from WSSTT.structs import Peer
from WSSTT.utils import logger

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
log_filename = sys.argv[sys.argv.index("-log") + 1] if "-log" in sys.argv else "AMSC.log"
coinbase_se = int(sys.argv[sys.argv.index("-coinbase_se") + 1] if "-coinbase_se" in sys.argv else 1)

# Create DB

db = Database(db_num=db_num)

if "-reset" in sys.argv:
    db.redis.flushdb()

# Initialize P2P

seeds = [(extra_seed[0], int(extra_seed[1]))]
p2p = Network(seeds=seeds, address=('0.0.0.0', port), debug=True)

# Chain

root_block = SimpleBlock(links=[], work_target=10**6, total_work=10**6, timestamp=1412226468, nonce=529437, coinbase=PUB_KEY_X_FOR_KNOWN_SE, state_hash=110737787655952643436062828845269098869204940693353997171788395014951100605706)
chain = Chain(root_block, db, p2p)

# Handlers

set_message_handlers(chain, p2p)

# inbuilt miner

coinbase_miner = pubkey_for_secret_exponent(coinbase_se)[0]  # get x coord

miner = Miner(chain, p2p, coinbase_miner)

logging.basicConfig(filename=log_filename, level=logging.DEBUG)

# Create root
if "-create_root" in sys.argv:
    root = SimpleBlock(links=[], work_target=10**6, total_work=10**6, timestamp=int(time.time()), nonce=miner._special_nonce, coinbase=PUB_KEY_X_FOR_KNOWN_SE, state_hash=110737787655952643436062828845269098869204940693353997171788395014951100605706)
    print(miner.mine_this_block(root).to_json())
    sys.exit()

# Go time!

try:
    start_threads(chain, p2p)
    if "-fast" in sys.argv: miner.mine_fast()
    if "-mine" in sys.argv: fire(miner.run)
    p2p.run()
except KeyboardInterrupt:
    pass
finally:
    print('TERMINATING: DO NOT CLOSE')
    miner.stop()
    p2p.shutdown()