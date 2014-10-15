--[[
  LUA Scripts for data structures.
  Used to create high speed complex structures in redis.
  Debugging Instructions: http://www.trikoder.net/blog/make-lua-debugging-easier-in-redis-87/
]]


--[[ INTERNAL ORGANISATION ]]

--[[ path augmentations for datastructs ]]

local _set = ".s"  -- set of orphan hashes
local _list = ".l"  -- map of orphan hashes to serialized blocks

--[[ generators for those paths ]]

local gen_set_path = function (path)
  return path .. _set
end

local gen_list_path = function (path)
  return path .. _list
end

--[[ FUNCTIONS FOR SUPPORTED OPERATIONS ]]

--[[ Lookups ]]

local chain_contains = function (path, block_hash)
  return redis.call("SISMEMBER", gen_set_path(path), block_hash)
end

local chain_get_all = function (path)
    return redis.call("LRANGE", gen_list_path(path), 0, -1)
end

--[[ Modification ]]

local chain_append = function (path, block_hashes)
  for i, hash in ipairs(block_hashes) do
      redis.call("SADD", gen_set_path(path), hash)
      redis.call("RPUSH", gen_list_path(path), hash)
  end
end

local chain_remove = function (path, block_hashes)
  for i, hash in ipairs(block_hashes) do
      redis.call("SREM", gen_set_path(path), hash)
      redis.call("RPOP", gen_list_path(path))
  end
end