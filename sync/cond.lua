local fiber = require "fiber"

---@class sync.cond
---@field name string?
---@field timeout number?
---@field sent boolean? true if condvar was already used
local cond = {}
cond.__index = cond
cond.__tostring = function (self) return "cond<".. (self.name or 'anon') ..">" end
setmetatable(cond, { __call = function (_, ...) return _.new(...) end })

---Returns new cond
---@param name string? name of the cond (default=anon)
---@param timeout number? default wait timeout (default=infinity)
---@return sync.cond
function cond.new(name, timeout)
	if name == cond then error("Usage: cond.new([name]) or cond([name]) (not cond:new())", 2) end
	return setmetatable({
		name    = name;
		timeout = timeout;
	}, cond)
end

---Sends data to condvar. If data was already sent then raises and error
---@param data any
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

---Awaits data on condvar for timeout
---@param timeout number? default to cond.timeout or inifinity
---@return any, string? error_message
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
	self.lock:wait(timeout or self.timeout)
	self.lock = nil
	if self.sent then
		return self.value
	else
		return nil, "Timed out"
	end
end

return cond;
