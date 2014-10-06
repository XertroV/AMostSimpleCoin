
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
        redis.call("HMSET", to, unpack(redis.call("HGETALL", from)))
    end
end


"""

def make_script(r, lua_code, path, **kwargs):
    #print("Making", lua_code.format(path=path, **kwargs))
    return r.register_script(_functions + lua_code.format(path=path, **kwargs))


_backup_state = """
    copy_hashmap('{path}', '{backup_path}')
"""

def get_backup_state_function(r, path, backup_path):
    return make_script(r, _backup_state, path, backup_path=backup_path)

_restore_backup_state = """
    copy_hashmap('{backup_path}', '{path}')
    redis.call("DEL", '{backup_path}')
"""

def get_restore_backup_state_function(r, path, backup_path):
    return make_script(r, _restore_backup_state, path, backup_path=backup_path)

_mod_balance = """
    local n
    local i
    local new_val
    n = table.getn(KEYS)
    for i=1, n do
        new_val = get_balance("{path}", KEYS[i]) + ARGV[i]
        assert(new_val >= 0, "Cannot allow negative balance")
        redis.call("HSET", "{path}", KEYS[i], new_val)
    end
    """

def get_mod_balance(r, path):
    return make_script(r, _mod_balance, path)

_get_balance = """
    return get_balance("{path}", KEYS[1])
    """

def get_get_balance(r, path):
    return make_script(r, _get_balance, path)