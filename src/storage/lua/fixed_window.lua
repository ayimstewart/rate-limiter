-- Fixed window counter.
--
-- ABI: see token_bucket.lua.
--
-- State: one integer counter per (key, window). The key is KEYS[1] .. ':' .. window_start_ms,
-- computed here from the same clock that makes the decision, so the window boundary is
-- identical for every caller. (This builds a key name inside the script, so on Redis
-- Cluster wrap the logical key in a {hash tag}; see the README.)
-- reset_ms = end of the current window.

local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[4])
if now_ms == nil or now_ms < 0 then
  local t = redis.call('TIME')
  now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local window_start = now_ms - (now_ms % window_ms)
local key = KEYS[1] .. ':' .. string.format('%.0f', window_start)

local count = tonumber(redis.call('GET', key) or '0')

local allowed = 0
local retry_ms = 0
if count < limit then
  count = redis.call('INCR', key)
  if count == 1 then
    redis.call('PEXPIRE', key, window_ms)
  end
  allowed = 1
else
  retry_ms = window_start + window_ms - now_ms
end

return {allowed, math.max(0, limit - count), window_start + window_ms, retry_ms, 0}
