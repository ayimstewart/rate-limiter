-- Token bucket.
--
-- One ABI for every script in this directory:
--   KEYS[1]  state key
--   ARGV[1]  limit      requests allowed per window (== bucket capacity)
--   ARGV[2]  window_ms  window length in milliseconds (integer)
--   ARGV[3]  request id unique per call (the sliding log needs it as a ZSET member)
--   ARGV[4]  now_ms     caller-supplied clock, or -1 to use the Redis server clock
-- Returns {allowed, remaining, reset_ms, retry_ms, delay_ms}; every element is an integer.
--
-- State (hash): `tokens` float, `ts` ms of the last refill.
-- reset_ms = when the bucket is full again.
--
-- The whole read-refill-decide-write cycle runs inside one EVALSHA, so concurrent
-- callers (any number of app instances) can never interleave between the read and the write.

local EPS = 1e-9

local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[4])
if now_ms == nil or now_ms < 0 then
  -- One time source for every client: the Redis server. App-server clock skew is irrelevant.
  local t = redis.call('TIME')
  now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local rate = limit / window_ms -- tokens per millisecond

local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local last = tonumber(state[2])

if tokens == nil or last == nil then
  tokens = limit -- first sight of this key: full bucket
else
  -- Clock stepped backwards: re-anchor instead of refilling by a negative amount
  -- (or stalling until the old timestamp is reached again).
  if now_ms < last then
    last = now_ms
  end
  tokens = math.min(limit, tokens + (now_ms - last) * rate)
end

local allowed = 0
local retry_ms = 0
if tokens >= 1 - EPS then
  tokens = math.max(0, tokens - 1)
  allowed = 1
else
  retry_ms = math.ceil((1 - tokens) / rate)
end

redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', now_ms)
-- A bucket left alone for a full window is full again, i.e. indistinguishable from a new key.
redis.call('PEXPIRE', KEYS[1], window_ms)

return {allowed, math.floor(tokens + EPS), now_ms + math.ceil((limit - tokens) / rate), retry_ms, 0}
