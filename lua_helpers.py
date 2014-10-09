
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

# Backup / restore

_backup_state = """
    copy_hashmap('{path}', KEYS[1])
"""

def get_backup_state_function(r, path, backup_path):
    return make_script(r, _backup_state, path, backup_path=backup_path)

_restore_backup_state = """
    copy_hashmap(KEYS[1], '{path}')
    redis.call("DEL", KEYS[1])
"""

def get_restore_backup_state_function(r, path, backup_path):
    return make_script(r, _restore_backup_state, path, backup_path=backup_path)

# Balance

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

_get_balance = """
    return get_balance("{path}", KEYS[1])
    """

def get_get_balance(r, path):
    return make_script(r, _get_balance, path)


#
# Orphanage

"""
local orph_contains = function (path, block_hash)
  return redis.call("SISMEMBER", gen_set_path(path), block_hash)
end

local orph_get = function(path, block_hash)
  return redis.call("HGET", gen_orph_map_path(path), block_hash)
end

local orphs_linking_to = function(block_hash)
  local set_key = redis.call("HGET", gen_rev_map_path(path), block_hash)
  return redis.call("SMEMBERS", gen_rev_key_path(path, block_hash))
end

--[[ Modification ]]

local orph_add = function (path, block, block_hash, linked_block_hash)
  redis.call("SADD", gen_set_path(path), block_hash)
  redis.call("HSET", gen_orph_map_path(path), block_hash, block)
  redis.call("HSET", gen_rev_map_path(path), linked_block_hash, gen_rev_key_path(path, linked_block_hash))
  redis.call("SADD", gen_rev_key_path(path, linked_block_hash), block_hash)
end

local orph_remove = function (path, block_hash)
  redis.call("SREM", gen_set_path(path), block_hash)
  redis.call("HDEL", gen_orph_map_path(path), block_hash)
  redis.call("SREM", gen_rev_key_path(path, linked_block_hash), block_hash)
  if redis.call("EXISTS", gen_rev_key_path(path, linked_block_hash)) == 0 then
    redis.call("HDEL", gen_rev_map_path(path), linked_block_hash, gen_rev_key_path(path, linked_block_hash))
  end
end"""
#

with open('lua_scripts/orphanage.lua') as f:
    orphanage_functions = f.read()

def make_orphanage_script(r, lua_code, path):
    return make_script(r, orphanage_functions + lua_code, path)


_orphanage_add = """
    return orph_add("{path}", KEYS[1], KEYS[2], KEYS[3])
"""

def get_orphanage_add(r, path):
    # KEYS[1,2,3] are block, block_hash, linked_block_hash respectively
    return make_orphanage_script(r, _orphanage_add, path)

_orphanage_add