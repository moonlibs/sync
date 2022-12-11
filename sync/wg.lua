local fiber = require "fiber"

local wg = {}
wg.__index = wg
wg.__tostring = function (self) return "wg<".. (self.name or 'anon') ..">" end
setmetatable(wg, { __call = function (_, name) return _.new(name) end })

function wg.new(name, timeout)
	if name == wg then error("Usage: wg.new([name]) or wg([name]) (not wg:new())", 2) end
	return setmetatable({
		name    = name;
		timeout = timeout;

		active = 0;
		completed = nil;
	}, wg)
end

function wg:start(n)
	if getmetatable( self ) ~= wg then
		error("Usage: wg:start() (not wg.start())", 2)
	end
	self.active = self.active + (n or 1)
	self.completed = false;
end
wg.add = wg.start

function wg:finish()
	if getmetatable( self ) ~= wg then
		error("Usage: wg:finish() (not wg.finish())", 2)
	end
	if self.active == 0 then
		error("wg:finish leads to negative count. start and finish are not balanced ", 2)
	end
	self.active = self.active - 1
	if self.active == 0 then
		self.completed = false;
		if self.lock then
			self.lock:broadcast()
		end
	end
end
wg.done = wg.finish

function wg:wait(timeout)
	if getmetatable( self ) ~= wg then
		error("Usage: wg:wait([timeout]) (not wg.wait([timeout]))", 2)
	end
	if self.active == 0 or self.completed then
		return true
	end
	if not self.lock then
		self.lock = fiber.cond()
	end
	self.lock:wait(timeout or self.timeout)
	self.lock = nil
	if self.active == 0 or self.completed then
		return true
	else
		return nil, "Timed out"
	end
end

return wg
