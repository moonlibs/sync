--[[
    tarantool/mysql:

        local latch = fiber.channel(1)
        local conn
        fiber.create(function()
            conn = pool:get()
            latch:put(true)
        end)
        local res = latch:get(1)
        test:is(res, nil, 'unable to get more connections then a pool size')

        -- Give a connection back and verify that now the fiber
        -- above gets this connection.
        pool:put(table.remove(connections))
        latch:get()
        test:ok(conn ~= nil, 'able to get a connection when it was given back')


    tarantool core:
        https://github.com/tarantool/tarantool/blob/5404abaa49a18514daf8c955a4e0b30c9e92931d/src/lib/core/latch.h

]]

local ffi = require 'ffi'
local defs = {
    { 'box_latch_t', [[ typedef void box_latch_t; ]] },
    { 'box_latch_new', [[ box_latch_t* box_latch_new(void); ]] },
    { 'box_latch_delete', [[ void box_latch_delete(box_latch_t* bl); ]] },
    { 'box_latch_lock', [[ void box_latch_lock(box_latch_t* bl); ]] },
    { 'box_latch_trylock', [[ int box_latch_trylock(box_latch_t* bl); ]] },
    { 'box_latch_unlock', [[ void box_latch_unlock(box_latch_t* bl); ]] },
}

local C = ffi.C

for _=1,2 do
for _, def in pairs(defs) do
    local key, cdef = unpack(def);
    if not pcall(function(k) return C[k] end, key) then
        ffi.cdef(cdef)
    end
end
end

---Latch is bindings to lightweight tarantool locks
---@class sync.latch
---@field name string? name of the latch if was given
---@field obj ffi.ctype* latch object itself
local latch = {}
latch.__index = latch
latch.__tostring = function (self) return "latch<".. (self.name or 'anon') ..">" end
setmetatable(latch, { __call = function (_, name) return _.new(name) end})

local function destroy(obj)
    C.box_latch_unlock(obj) -- unlock by gc
    C.box_latch_delete(obj)
end

---Creates new latch
---@param name string? name of the latch
---@return sync.latch
function latch.new(name)
    if name == latch then error("Usage: latch.new([name]) or latch([name]) (not latch:new())", 2) end
    local obj = C.box_latch_new()
    if not obj then error("Failed to create latch") end
    ffi.gc(obj, destroy)

    return setmetatable({
        obj     = obj;
        name    = name;
    }, latch)
end

---Locks latch
---
---this method does not accept timeout, so can be locked infinetely long
function latch:lock()
    if getmetatable( self ) ~= latch then
        error("Usage: latch:lock() (not latch.lock())", 2)
    end
    C.box_latch_lock(self.obj)
end

---Fast checks if latch can be locked
---@return boolean success # returns true if latch was captured
function latch:trylock()
    if getmetatable( self ) ~= latch then
        error("Usage: latch:trylock() (not latch.trylock())", 2)
    end
    return C.box_latch_trylock(self.obj) == 0 -- 0 means success
end

---Releases locked latch
function latch:unlock()
    if getmetatable( self ) ~= latch then
        error("Usage: latch:unlock() (not latch.unlock())", 2)
    end
    C.box_latch_unlock(self.obj)
end

local function tail(self, r, ...)
    self:unlock()
    if not r then
        error((...), 3)
    end
    return ...
end

---Wrapper lock()/unlock() which executes `f`
---
---latch will be released whether function raised exception or not
---@param f fun(...:any) callable object, usually function of the critical section
---@param ... any? arguments of the function
---@return ... # return result of the function or raises exception
function latch:with(f, ...)
    if getmetatable( self ) ~= latch then
        error("Usage: latch:with(fn) (not latch.with(fn))", 2)
    end
    self:lock()
    return tail(self, pcall(f, ...))
end


return latch
