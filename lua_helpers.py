
_functions = """
local get_balance = function (path, pub_x)
    if redis.call("HEXISTS", path, pub_x) == 1 then
        return redis.call("HGET", path, pub_x)
    else
        return 0
    end
end

local copy_hashmap = function (from, to)
    if redis.call("EXISTS", from) == 1 then
        redis.call("DEL", to)
        redis.call("HMSET", to, unpack(redis.call("HGETALL", from)))
    end
end
"""

def make_script(r, lua_code, path, **kwargs):
    #print("Making", lua_code.format(path=path, **kwargs))
    return r.register_script(_functions + lua_code.format(path=path, **kwargs))


#
# State
#

# Backup
_backup_state = """
    copy_hashmap('{path}', KEYS[1])
"""
def get_backup_state_function(r, path, backup_path):
    return make_script(r, _backup_state, path, backup_path=backup_path)

# Restore
_restore_backup_state = """
    copy_hashmap(KEYS[1], '{path}')
    redis.call("DEL", KEYS[1])
"""
def get_restore_backup_state_function(r, path, backup_path):
    return make_script(r, _restore_backup_state, path, backup_path=backup_path)

# Balance, modify
_mod_balance = """
    local n
    local i
    local new_val
    n = table.getn(KEYS)
    for i=1, n do
        new_val = get_balance("{path}", KEYS[i]) + ARGV[i]
        assert(new_val >= 0, "Cannot allow negative balance")
        if (new_val > 0) then
            redis.call("HSET", "{path}", KEYS[i], new_val)
        else
            redis.call("HDEL", "{path}", KEYS[i])
        end
    end
"""
def get_mod_balance(r, path):
    return make_script(r, _mod_balance, path)

# Balance, get
_get_balance = """
    return get_balance("{path}", KEYS[1])
    """
def get_get_balance(r, path):
    return make_script(r, _get_balance, path)


#
# Orphanage
#

with open('lua_scripts/orphanage.lua') as f:
    orphanage_functions = ''.join(f.readlines())

def make_orphanage_script(r, lua_code, path):
    #print("Script", orphanage_functions + lua_code)
    return make_script(r, orphanage_functions + lua_code, path)

# Membership Test
_orphanage_contains = """
    return orph_contains("{path}", KEYS[1])
"""
def get_orphanage_contains(r, path):
    # KEYS[1] is the block hash
    return make_orphanage_script(r, _orphanage_contains, path)

# Get
_orphanage_get = """
    return orph_get("{path}", KEYS[1])
"""
def get_orphanage_get(r, path):
    # KEYS[1] is the block_hash
    return make_orphanage_script(r, _orphanage_get, path)

# Get linking block hashes
_orphanage_linking_to = """
    return orphs_linking_to("{path}", KEYS[1])
"""
def get_orphanage_linking_to(r, path):
    # KEYS[1] is the block_hash of a block linked to by orphans. A set of orphan hashes is returned
    return make_orphanage_script(r, _orphanage_linking_to, path)

_orphanage_add = """
    return orph_add("{path}", KEYS[1], KEYS[2], KEYS[3])
"""
def get_orphanage_add(r, path):
    # KEYS[1,2,3] are block, block_hash, linked_block_hash respectively
    return make_orphanage_script(r, _orphanage_add, path)


_orphanage_remove = """
    return orph_remove("{path}", KEYS[1], KEYS[2])
"""
def get_orphanage_remove(r, path):
    # KEYS[1] is the block_hash, linked_block_hash
    return make_orphanage_script(r, _orphanage_remove, path)


