local fiber = require 'fiber'
local log = require 'log'

local sync = {}
do
	local cv = setmetatable({},{
		__call = function(cls)
			local new = setmetatable({
				count = 0;

			},{__index = cls})
			return new
		end,
	});

	function cv:start()
		self.count = self.count + 1
	end

	function cv:finish()
		self.count = self.count - 1
		if self.count == 0 then
			if self.channel then
				self.channel:put(true,0)
			elseif self.count < 0 then
				error("condvar count gone below 0")
			end
		end
	end

	function cv:wait()
		if self.count == 0 then return end
		if not self.channel then
			self.channel = fiber.channel(1)
		end
		self.channel:get()
		self.channel = nil
		return
	end

	sync.cv = cv;

end
return sync
