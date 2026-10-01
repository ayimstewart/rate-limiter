-- Sliding window log.
--
-- ABI: see token_bucket.lua.
--
-- State (sorted set): one member per admitted request, scored by its timestamp in ms.
-- Memory is O(limit) per key: this is the price of an exact count.
-- reset_ms = when the newest entry leaves the window (i.e. the log is empty again).

local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[4])
if now_ms == nil or now_ms < 0 then
  local t = redis.call('TIME')
  now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local key = KEYS[1]

-- An entry counts while now - ts < window, so it is evicted when ts <= now - window.
redis.call('ZREMRANGEBYSCORE', key, '-inf', now_ms - window_ms)
local count = redis.call('ZCARD', key)

local allowed = 0
local retry_ms = 0
if count < limit then
  -- ARGV[3] is unique per call; two requests in the same millisecond must not collapse into one member.
  redis.call('ZADD', key, now_ms, ARGV[3])
  redis.call('PEXPIRE', key, window_ms)
  count = count + 1
  allowed = 1
else
  local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
  retry_ms = math.max(0, tonumber(oldest[2]) + window_ms - now_ms)
end

local reset_ms = now_ms
local newest = redis.call('ZRANGE', key, -1, -1, 'WITHSCORES')
if newest[2] ~= nil then
  reset_ms = tonumber(newest[2]) + window_ms
end

return {allowed, math.max(0, limit - count), reset_ms, retry_ms, 0}
