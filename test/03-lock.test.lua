#!/usr/bin/env tarantool

local fiber = require 'fiber'
local test = require('test.ex').test('lock')

local sync = require('sync')

test:ok(sync.lock.new(), 'anon new by method' )
test:ok(sync.lock(), 'anon new by call' )
test:ok(sync.lock.new('test1'), 'named new by method')
test:ok(sync.lock('test2'), 'named new by call')

local function wrap(f, ch, ...)
	local r, e = pcall(f, ...)
	assert(ch:put(fiber.id(), 0)) -- must have space, so must not yield
	if not r then error(e) end -- rethrow
end

for N = 2, 5 do -- FIXME -> 5
	local locks = {}
	---@type table<integer, Fiber>
	local fibers = {}
	-- local N = 5
	local chan = fiber.channel(N)
	local ok = 0

	for i = 1, N do
		locks[i] = sync.lock(tostring(i))

		local f = fiber.new(wrap, function(arg)
			-- print("call to", arg, fiber.id())

			locks[i]:aquire()
			fiber.sleep(0.1)
			locks[i%N + 1]:aquire()

			-- print("Success")
			test:diag("fiber %s aquired 2 locks", fiber.id())

			ok = ok + 1

			locks[i%N + 1]:release()
			locks[i]:release()
		end, chan, "arg")
		f:set_joinable(true)
		fibers[f:id()] = f
	end
	test:deadline(function()
		local one_err
		while next(fibers) do
			local id = chan:get()
			-- print("wakeup", id)
			local fib = fibers[id]
			if fib then
				local r, e = fib:join()
				if not r then
					if one_err then
						test:fail("Expected one error, but got more")
						test:diag(one_err)
					end
					one_err = e
				end
				fibers[id] = nil
			else
				error("Fiber not found")
			end
		end
		chan:close()
		test:like(one_err, "Deadlock detected", "One deadlock has been cought")
		test:is(ok, N-1, "Others "..(N-1).." are succeded")
	end, 1, "wait for "..N )
end

do
	test:diag("Test from synchronized")

	local fiber = require('fiber')

	local in_action = 0
	local function criticalsection(id)
		test:diag("call to crit section %d", id)
		in_action = in_action + 1
		test:is(in_action, 1, "in action only 1 fiber in "..id)
		fiber.sleep(0.1)
		test:is(in_action, 1, "in action only 1 fiber in "..id)
		in_action = in_action - 1
	end
	local lock = sync.lock('somekey')

	local N = 3
	local ch = fiber.channel(N)
	for i=1, N do
		fiber.create(function(id)
			lock:with(criticalsection, id)
			ch:put(true)
		end, i)
	end

	for _=1, N do
		ch:get()
	end
end

test:raises(function ()
	sync.lock:new()
end, 'wrong constructor call')

test:raises(function ()
	local lock = sync.lock()
	lock.aquire()
end, 'Usage: lock:aquire%(%) %(not lock%.aquire%(%)%)', 'static call aquire on object')

test:raises(function ()
	local lock = sync.lock()
	lock.release()
end, 'Usage: lock:release%(%) %(not lock%.release%(%)%)', 'static call release on object')

test:raises(function ()
	local lock = sync.lock()
	lock.with(function() end)
end, 'Usage: lock:with%(fn%) %(not lock%.with%(fn%)%)', 'static call with on object')

test:deadline(function()
	local lock = sync.lock()
	lock:aquire()
	lock:release()
end, 3, 'initial aquire would not block')

test:deadline(function()
	local lock = sync.lock()
	local completed = 0
	-- local N = 10
	-- local M = 100
	local N = 2
	local M = 10
	local value = 0

	local fibers = {}

	for _ = 1, N do
		local fib = fiber.new(function ()
			for x=1,M do

				lock:aquire()

				-- simulate yielding non-atomic change
				local val = value
				fiber.sleep( math.random()/1e2 )
				-- value may have changed, while val keeps local copy
				value = val + 1
				fiber.sleep( math.random()/1e2 )

				lock:release()

			end
			completed = completed + 1
		end)
		fib:set_joinable(true);
		table.insert(fibers, fib)
	end

	for _, fib in pairs(fibers) do fib:join() end

	test:is( value, N * M, 'all changes are non concurrent' )
	test:is( completed, N, 'all completed' )
end, 3, 'concurrent locks')

test:done_testing()
