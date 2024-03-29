local fiber = require "fiber"

---@class sync.lock
---@field name string name of the lock
---@field locked false|number false when no one acquired lock, `fiber_id` otherwise
---@field _lock fiber.cond
local lock = {
    WAIT_WINDOW = 1/3,
}

lock.__index = lock
lock.__tostring = function (self) return "lock<".. (self.name or 'anon') ..">" end
setmetatable(lock, { __call = function (_, name) return _.new(name) end })

local FIBER_STORE = 'sync.lock'

---Creates new lock
---@param name string? name of the lock
---@return sync.lock
function lock.new(name)
	if name == lock then error("Usage: lock.new([name]) or lock([name]) (not lock:new())", 2) end
	return setmetatable({
		name    = name;
        locked  = false;
	}, lock)
end

function lock:_self_check()
    if not self.locked then return end
    local locker = fiber.find(self.locked)
    if not locker then
        io.stderr:write(("sync.lock: fiber %s with lock %s is absent. autorelease\n"):format(self.locked, self))
        self:release()
    end
end

---Tries to acquire lock
---
---Returns `true` on success, returns `false` on timed out
---
---Raises exception when attempting to acquire same lock twice in the same fiber
---
---Raises exception when deadlock is discovered
---@param timeout? number timeout in seconds, default timeout is infinity
---@return boolean # `true` if lock was successfully captured, `false` whe timed out
function lock:aquire(timeout)
	if getmetatable( self ) ~= lock then
		error("Usage: lock:aquire() (not lock.aquire())", 2)
	end
    -- print("[".. tostring(self) .. "] aquire", self.locked, timeout, fiber.id());
    local deadline = timeout and fiber.time() + timeout
    while self.locked do
        local locker = fiber.find(self.locked)
        if not locker then
            self:_self_check()
            -- io.stderr:write(("sync.lock: fiber %s with lock %s is absent. autorelease\n"):format(self.locked, self))
            -- self:release()
        elseif self.locked == fiber.id() then
            error("Failed to lock the same lock twice", 2)
        elseif timeout == 0 then
            return false
        else
            if not self._lock then
                self._lock = fiber.cond()
            end
            -- print(string.format("[%s] locked by fiber %s, requested by %s", self, locker:id() or '-', fiber.id()))
            -- print("\tblocking", fiber.id(), " on lock ", self)

            fiber.self().storage[FIBER_STORE] = fiber.self().storage[FIBER_STORE] or {}
            fiber.self().storage[FIBER_STORE][ self ] = self

            -- print("\t\twaiting", fiber.id(), " on lock ", self, i)

            -- deadlock check:
            local seen = {}
            local fibers_to_check = { locker }
            while #fibers_to_check > 0 do
                local check = table.remove( fibers_to_check, 1 )
                seen[ check:id() ] = true
                local their_locks = check.storage[FIBER_STORE]
                -- print("\tcheck locks of ", check:id())
                if their_locks then
                    for l in pairs(their_locks) do
                        -- print("\t\tlocker ",check:id()," waits for lock: ", l, "but it is locked by ", l.locked)
                        local fib = fiber.find(l.locked)
                        if fib then
                            if fib:id() == fiber.id() then
                                if l._lock then
                                    l._lock:signal()
                                end
                                -- print("deadlock in ",fiber.id())
                                local msg = ("Deadlock detected: fiber %s:%s requested %s, but already locked by %s:%s")
                                    :format(fiber.id(), fiber.self():name(), self, locker:id(), locker:name() )
                                fiber.self().storage[FIBER_STORE][ self ] = nil
                                io.stderr:write(msg.."\n")
                                error(msg, 2)
                            end
                            if not seen[ fib:id() ] then
                                table.insert(fibers_to_check, fib)
                            end
                        else
                            l:_self_check()
                        end
                    end
                end
            end
            local left = deadline and deadline - fiber.time()
            if not left or left > lock.WAIT_WINDOW then left = lock.WAIT_WINDOW end
            self._lock:wait(left)

            fiber.self().storage[FIBER_STORE][ self ] = nil
        end
    end

    self.locked = fiber.id()
    return true
end
lock.lock = lock.aquire

---Releases lock
---
---Raises exception when lock is acquired by noone
---
---Does not check lock ownership
function lock:release()
	if getmetatable( self ) ~= lock then
		error("Usage: lock:release() (not lock.release())", 2)
	end
	if not self.locked then
		error("lock:release called on not aquired lock", 2)
	end
    self.locked = false
    if self._lock then
        self._lock:signal()
    end
end
lock.unlock = lock.release

local function tail(self, r, ...)
    self:release()
    if not r then
        error((...), 3)
    end
    return ...
end

---Executes given function under lock
---
---:with() reraises exception if function raised exception
---lock will be automatically released regardless success of function execution
---@param f fun() function of the critical section
---@param ... any arguments for the function
---@return ... # returns result of the function
function lock:with(f, ...)
	if getmetatable( self ) ~= lock then
		error("Usage: lock:with(fn) (not lock.with(fn))", 2)
	end
    self:aquire()
    return tail(self, pcall(f, ...))
end

return lock