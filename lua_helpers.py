def load_lua_file(filename):
    try:
        with open('lua_scripts/%s' % filename) as f:
            return ''.join(f.readlines())
    except:  # todo : this is for tests, shouldn't be included forever though
        with open('../lua_scripts/%s' % filename) as f:
            return ''.join(f.readlines())


def make_script_maker_with_functions(functions):
    def maker(r, lua_code, path):
        return make_script(r, functions + lua_code, path)
    return maker


# This section is a bit messy...
# Usually a section will have some functions and that string is prepended to all commands. It's a cheap library,
# sort of. Usually the specific redis function will be _some_name, the function factory thing is get_some_name.


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


orphanage_functions = load_lua_file('orphanage.lua')

make_orphanage_script = make_script_maker_with_functions(orphanage_functions)

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
    # KEYS[1] is the block_hash, KEYS[2] : linked_block_hash
    return make_orphanage_script(r, _orphanage_remove, path)


#
# Primary Chain
#

_primary_chain_functions = load_lua_file('primary_chain.lua')

make_primary_chain_script = make_script_maker_with_functions(_primary_chain_functions)

_primary_chain_contains = """
    return chain_contains("{path}", KEYS[1])
"""
def get_primary_chain_contains(r, path):
    # KEYS[1] is block_hash
    return make_primary_chain_script(r, _primary_chain_contains, path)

_primary_chain_get_all = """
    return chain_get_all("{path}")
"""
def get_primary_chain_get_all(r, path):
    return make_primary_chain_script(r, _primary_chain_get_all, path)

_primary_chain_append = """
    return chain_append("{path}", KEYS)
"""
def get_primary_chain_append(r, path):
    return make_primary_chain_script(r, _primary_chain_append, path)

_primary_chain_remove = """
    return chain_remove("{path}", KEYS)
"""
def get_primary_chain_remove(r, path):
    return make_primary_chain_script(r, _primary_chain_remove, path)