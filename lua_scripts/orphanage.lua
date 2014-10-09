--[[
  LUA Scripts for data structures.
  Used to create high speed complex structures in redis.
  Debugging Instructions: http://www.trikoder.net/blog/make-lua-debugging-easier-in-redis-87/
]]


--[[ INTERNAL ORGANISATION ]]

--[[ path augmentations for datastructs ]]

local _set = ".s"  -- set of orphan hashes
local _orph_map = ".m"  -- map of orphan hashes to serialized blocks
local _rev_map = ".r"  -- hash map of pointers to sets for reverse block links
local _rev_key_suffix = ".orph_rev"  -- suffix for above pointer

--[[ generators for those paths ]]

local gen_set_path = function (path)
  return path .. _set
end

local gen_orph_map_path = function (path)
  return path .. _orph_map
end

local gen_rev_map_path = function (path)
  return path .. _rev_map
end

local gen_rev_key_path = function (path, block_hash)
  return path .. block_hash .. _rev_key_suffix
end

--[[ FUNCTIONS FOR SUPPORTED OPERATIONS ]]

--[[ Lookups ]]

local orph_contains = function (path, block_hash)
  return redis.call("SISMEMBER", gen_set_path(path), block_hash)
end

local orph_get = function(path, block_hash)
  return redis.call("HGET", gen_orph_map_path(path), block_hash)
end

local orphs_linking_to = function(path, block_hash)
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

local orph_remove = function (path, block_hash, linked_block_hash)
  redis.call("SREM", gen_set_path(path), block_hash)
  redis.call("HDEL", gen_orph_map_path(path), block_hash)
  redis.call("SREM", gen_rev_key_path(path, linked_block_hash), block_hash)
  if redis.call("EXISTS", gen_rev_key_path(path, linked_block_hash)) == 0 then
    redis.call("HDEL", gen_rev_map_path(path), linked_block_hash, gen_rev_key_path(path, linked_block_hash))
  end
end