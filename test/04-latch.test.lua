#!/usr/bin/env tarantool

local fiber = require 'fiber'
local test = require('test.ex').test('lock')

local sync = require('sync')

test:ok(sync.latch.new(), 'anon new by method' )
test:ok(sync.latch(), 'anon new by call' )
test:ok(sync.latch.new('test1'), 'named new by method')
test:ok(sync.latch('test2'), 'named new by call')

test:raises(function ()
	sync.latch:new()
end, 'wrong constructor call')

test:raises(function ()
	local lock = sync.latch()
	lock.lock()
end, 'Usage: latch:lock%(%) %(not latch%.lock%(%)%)', 'static call lock on object')

test:raises(function ()
	local lock = sync.latch()
	lock.unlock()
end, 'Usage: latch:unlock%(%) %(not latch%.unlock%(%)%)', 'static call unlock on object')

test:raises(function ()
	local lock = sync.latch()
	lock.trylock()
end, 'Usage: latch:trylock%(%) %(not latch%.trylock%(%)%)', 'static call trylock on object')

test:deadline(function()
	local lock = sync.latch()
	lock:lock()
	lock:unlock()
end, 3, 'initial lock would not block')

test:deadline(function()
	local lock = sync.latch()
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

				lock:lock()

				-- simulate yielding non-atomic change
				local val = value
				fiber.sleep( math.random()/1e2 )
				-- value may have changed, while val keeps local copy
				value = val + 1
				fiber.sleep( math.random()/1e2 )

				lock:unlock()

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

test:deadline(function()
	local lock = sync.latch()
	local completed = 0
	local N = 10
	local value = 0

	local fibers = {}

--[[
	check correctness for strict order.
	1. create a lot of fibers and lock them all in a sequential order.
	2. make short sleep after locking, then release.
	3. check, that locks aquired in a strict order of requesing
]]
	local prev = 0
	for i = 1, N do
		local fib = fiber.new(function (i)
			lock:lock()
			prev = prev + 1
			test:is(prev, i, "next lock aquired by "..i)
			fiber.sleep(0.01)
			lock:unlock()
		end, i)
		fib:set_joinable(true);
		table.insert(fibers, fib)
	end

	for _, fib in pairs(fibers) do fib:join() end

	test:is( prev, N, 'all completed' )
end, 3, 'strict seq constraint')

test:deadline(function()
	local lock = sync.latch()
	lock:lock()
	test:noyield(function ()
		lock:unlock()
	end)
end, 3, 'unlock no yield')

test:deadline(function()
	local lock = sync.latch()

	test:noyield(function ()
		test:is(lock:trylock(), true, "first trylock succeeded")
	end, 'trylock')
	test:noyield(function ()
		test:is(lock:trylock(), false, "second trylock failed")
	end, 'trylock')

	lock:unlock()
end, 3, 'trylock no yield')

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

test:done_testing()
