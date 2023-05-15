local fiber = require "fiber"

---@class sync.limit
---@field name string name of the limit
---@field limit number limit is events per second
---@field burst number burst is maximum number of tokens in limiter
---@field tokens number current tokens in the limiter
---@field last_ts number last timestamp when limiter was updated with tokens
---@field last_event number last timestamp of rate-limited event
local limit = {}

limit.__index = limit
limit.__tostring = function (self) return "limit<".. (self.name or 'anon') ..">" end
setmetatable(limit, { __call = function (_, name) return _.new(name) end })

---@class sync.limit.reservation
---@field lim sync.limit
---@field tokens number
---@field timeToAct number
local reservation_mt = {}
reservation_mt.__index = reservation_mt

---Cancels reservation. This means that requestor will not perform action under this reservation
---@param timestamp number timestamp (default=now())
function reservation_mt:cancel(timestamp)
    timestamp = tonumber(timestamp) or fiber.time()

    local lim = self.lim

    -- limiter is infinite
    if lim.limit == math.huge then return end

    -- no tokens to return
    if self.tokens == 0 then return end

    -- time of action already passed (nothing can be returned)
    if self.timeToAct < timestamp then return end

    local restore = self.tokens - (lim.last_event - self.timeToAct) * lim.limit
    if restore <= 0 then return end

    local tokens
    timestamp, tokens = lim:_advance(timestamp)

    tokens = math.min(lim.burst, tokens + restore)
    lim.last_ts = timestamp
    lim.tokens = tokens

    --- think about this:
    if self.timeToAct == lim.last_event then
        local prev_event = self.timeToAct + lim:_durationFromTokens(-self.tokens)
        if prev_event >= timestamp then
            lim.last_event = prev_event
        end
    end
end


---Creates new limit
---@param name string? name of the limit
---@param lim number float limit per second
---@param burst integer? allowed burst (default=0)
---@return sync.limit
function limit.new(name, lim, burst)
	if name == limit then error("Usage: limit.new([name]) or limit([name]) (not limit:new())", 2) end
    lim = tonumber(lim) or 0
    burst = math.floor(tonumber(burst) or 0)

    if lim < 0 then error("Usage: limit.new([name], limit, burst) limit must be non negative", 2) end
    if burst < 0 then error("Usage: limit.new([name], limit, burst) burst must be non negative", 2) end

	return setmetatable({
		name    = name;
        limit   = lim;
        burst   = burst or 0;
        tokens  = burst or 0;
        last_ts = 0;
        last_event = 0;
	}, limit)
end

---Calucalates number of tokens which will be available at time `t`
---@local
---@param timestamp number
function limit:_advance(timestamp)
    timestamp = assert(tonumber(timestamp))

    local elapsed = math.max(0, timestamp - self.last_ts)

    local delta
    if self.limit <= 0 then
        delta = 0
    else
        delta = self.limit * elapsed
    end

    return timestamp, math.min(self.burst, self.tokens + delta)
end

---Returns duration in fractinal seconds from token
---
---Can return `math.huge` is limit is non-positive
---@param tokens number
---@return number duration
function limit:_durationFromTokens(tokens)
    if self.limit <= 0 then
        return math.huge
    end

    return tokens / self.limit
end

---Reserves and advances limiter for requested tokens
---@local
---@param time number
---@param n number
---@param wait number
---@return boolean|sync.limit.reservation reservation, string? error_message
function limit:_reserve(time, n, wait)
    if self.limit == math.huge then
        return true
    end
    if self.limit == 0 then
        if self.burst >= n then
            self.burst = self.burst - n
            local r = setmetatable({
                ok = true,
                lim = self,
                tokens = self.burst,
                timeToAct = time,
            }, reservation_mt)
            return r
        end
        return false, "not enough burst"
    end

    if self.burst < n then
        return false, "not enough burst"
    end

    local tokens
    time, tokens = self:_advance(time)

    tokens = tokens - n

    local waitDuration = 0
    if tokens < 0 then
        -- not enough tokens
        waitDuration = self:_durationFromTokens(-tokens)
    end

    if waitDuration > wait then
        return false, "would exceed given timeout"
    end

    local r = setmetatable({
        lim = self,
        tokens = n,
        timeToAct = time+waitDuration,
    }, reservation_mt)

    -- update state
    self.last_ts = time
    self.tokens = tokens
    self.last_event = r.timeToAct

    return r
end

---Awaits limit until `n` events allowed within given timeout (default timeout=infinity)
---
---Can return instant `false` when required tokens can't be awaited in given `timeout`
---
---**Usage:**
---
---    -- wait for single event infinitely
---    assert(limit:wait())
---
---    -- await instant token (noyield)
---    if limit:wait(0) then
---        -- ratelimit granted
---    end
---
---    -- await 1 token within 100ms
---    assert(limit:wait(0.1))
---
---    -- await 2 tokens within 100ms
---    assert(limit:wait(0.1, 2))
---@async
---@param timeout number? timeout to wait
---@param n number?
---@return boolean success, string? error_message # true in case event was awaited, false otherwise
function limit:wait(timeout, n)
    timeout = tonumber(timeout) or math.huge
    n = tonumber(n) or 1

    if n > self.burst and self.limit ~= math.huge then
        return false, ("limit:wait(timeout=%s, n=%s) exceeds limiters burst=%s"):format(timeout, n, self.burst)
    end

    local now = fiber.time()
    local waitLim = math.min(timeout, math.huge)

    local r, err = self:_reserve(now, n, waitLim)
    if not r then
        return false, ("limit:wait(timeout=%s, n=%s) %s"):format(timeout, n, err)
    end

    local delay = math.max(0, r.timeToAct - now)
    if delay > 0 then
        fiber.sleep(delay)
    end

    return true
end

---Reports whether `n` events might be happen at time `timestamp`.
---
---Does not reserves tokens in limiter
---@param timestamp number? timestamp in seconds (default=now())
---@param n number? number of events required (default=1)
---@return boolean allowed, string? error_message
function limit:allow(timestamp, n)
    timestamp = tonumber(timestamp) or fiber.time()
    n = tonumber(n) or 1

    local r, err = self:_reserve(timestamp, n, 0)
    if not r then
        return false, err
    end
    return true
end

---Reserves `n` tokens at time `timestamp`
---@param timestamp number? timestamp in seconds (default=now())
---@param n? number number of events to be reserved (default=1)
---@return sync.limit.reservation|false, string? error_message
function limit:reserve(timestamp, n)
    timestamp = tonumber(timestamp) or fiber.time()
    n = tonumber(n) or 1

    return self:_reserve(timestamp, n, math.huge)
end


return limit