local fiber = require "fiber"

---@class sync.cond
---@field name string
---@field timeout number? default timeout for :recv()
---@field sent boolean? was data sent through condvar
local cond = {}
cond.__index = cond
cond.__tostring = function (self) return "cond<".. (self.name or 'anon') ..">" end
setmetatable(cond, { __call = function (_, ...) return _.new(...) end })

---Creates new sync.cond
---@param name string? name of sync.cond
---@param timeout number? default timeout for sync.recv
---@return sync.cond
function cond.new(name, timeout)
	if name == cond then error("Usage: cond.new([name]) or cond([name]) (not cond:new())", 2) end
	return setmetatable({
		name    = name;
		timeout = timeout;
	}, cond)
end

---Sends data to the cond and receiver
---
---Method raises exception if any data was already sent through this cond
---@param data any data you want to send (please do not send nil/box.NULL, it is acceptable but not usable)
function cond:send(data)
	if getmetatable( self ) ~= cond then
		error("Usage: cond:send(value) (not cond.send(value))", 2)
	end
	if self.sent then
		error("Condition variable already sent", 2)
	end
	self.sent = true
	self.value = data
	if self.lock then
		self.lock:broadcast()
	end
end

---Receives data from conditional variable
---@param timeout number? timeout in seconds to wait for data (default self.timeout or infinity)
---@return any|nil, string? error_message # returns `sent data` if was awaited, or nil, "Timed out" otherwise
function cond:recv(timeout)
	if getmetatable( self ) ~= cond then
		error("Usage: cond:send(value) (not cond.send(value))", 2)
	end
	if self.sent then
		return self.value
	end
	if not self.lock then
		self.lock = fiber.cond()
	end
	if timeout == 0 then
		return nil, "Timed out"
	end
	self.lock:wait(timeout or self.timeout)
	self.lock = nil
	if self.sent then
		return self.value
	else
		return nil, "Timed out"
	end
end

return cond;
