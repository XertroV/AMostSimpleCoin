local redis = require 'redis'
local dumpLib = require 'dump'

local dump = function(v) 
    if nil ~= v then
        print(dumpLib.tostring(v))
    end
end

-- If you have some different host/port change it here
local host = "127.0.0.1"
local port = 6379

client = redis.connect(host, port)

-- Workaround for absence of redis.call or atelast I did not find one
-- And did not want to digg in redis source code to see how does he get redis.call
redis.call = function(cmd, ...) 
    return assert(loadstring('return client:'.. string.lower(cmd) ..'(...)'))(...)
end

local run_debug = function (f, KEYS, ARGV)
    -- here goes your lua redis script
    -- with optional return if you have some
    dump(f(KEYS, ARGV))
end

-- Populate your ARGV and KEYS variables
local ARGV = {"test"}
local KEYS = {"keys"}

-- If you need to use some other DB uncomment next line and change number
redis.call("SELECT", 15)