-- Leaky bucket (as a meter with a delay output).
--
-- ABI: see token_bucket.lua.
--
-- State (hash): `level` float (requests currently "in the bucket"), `ts` ms of the last drain.
-- Requests pour in one unit at a time; the bucket drains at limit/window per second.
--   * a request is admitted if there is room for it (level + 1 <= limit)
--   * delay_ms = how long the caller should hold the request so that admitted requests
--     leave at the constant drain rate (the smoothing a leaky *queue* would do)
--   * reset_ms = when the bucket is empty again

local EPS = 1e-9

local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[4])
if now_ms == nil or now_ms < 0 then
  local t = redis.call('TIME')
  now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local rate = limit / window_ms -- drain per millisecond

local state = redis.call('HMGET', KEYS[1], 'level', 'ts')
local level = tonumber(state[1])
local last = tonumber(state[2])

if level == nil or last == nil then
  level = 0
else
  if now_ms < last then
    last = now_ms
  end
  level = math.max(0, level - (now_ms - last) * rate)
end

local allowed = 0
local retry_ms = 0
local delay_ms = 0
if level + 1 <= limit + EPS then
  delay_ms = math.ceil(level / rate)
  level = level + 1
  allowed = 1
else
  retry_ms = math.ceil((level + 1 - limit) / rate)
end

redis.call('HSET', KEYS[1], 'level', tostring(level), 'ts', now_ms)
-- Even a completely full bucket is empty after one window.
redis.call('PEXPIRE', KEYS[1], window_ms)

return {
  allowed,
  math.max(0, math.floor(limit - level + EPS)),
  now_ms + math.ceil(level / rate),
  retry_ms,
  delay_ms,
}
