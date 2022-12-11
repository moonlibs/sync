#!/usr/bin/env tarantool

local fiber = require 'fiber'
local test = require('test.ex').test('wg')

local sync = require('sync')

test:ok(sync.wg.new(), 'anon new by method' )
test:ok(sync.wg(), 'anon new by call' )
test:ok(sync.wg.new('test1'), 'named new by method')
test:ok(sync.wg('test2'), 'named new by call')

test:raises(function ()
	sync.wg:new()
end, 'wrong constructor call')

test:raises(function ()
	local wg = sync.wg()
	wg.start()
end, 'Usage: wg:start%(%) %(not wg%.start%(%)%)', 'static call start on object')

test:raises(function ()
	local wg = sync.wg()
	wg.add()
end, 'Usage: wg:start%(%) %(not wg%.start%(%)%)', 'static call start on object')

test:raises(function ()
	local wg = sync.wg()
	wg.finish()
end, 'Usage: wg:finish%(%) %(not wg%.finish%(%)%)', 'static call finish on object')

test:raises(function ()
	local wg = sync.wg()
	wg.done()
end, 'Usage: wg:finish%(%) %(not wg%.finish%(%)%)', 'static call finish on object')

test:raises(function ()
	local wg = sync.wg()
	wg.wait()
end, 'Usage: wg:wait%(%[timeout%]%) %(not wg%.wait%(%[timeout%]%)%)', 'static call wait on object')

test:deadline(function()
	local wg = sync.wg()
	wg:wait()
end, 3, 'wait on initial would not block')

test:deadline(function()
	local wg = sync.wg()
	local completed = 0
	local N = 3

	for _ = 1, N do
		wg:start()
		fiber.new(function ()
			fiber.sleep(0.1)
			completed = completed + 1
			wg:finish()
		end)
	end

	local res, err = wg:wait()

	test:is( res, true, 'reveived success' )
	test:is( completed, N, 'all completed' )
end, 3, 'recv -> send works')

test:deadline(function()
	local wg = sync.wg()
	local completed = 0
	local N = 3

	for _ = 1, N do
		wg:start()
		fiber.new(function ()
			fiber.sleep(0.1)
			completed = completed + 1
			wg:finish()
		end)
	end

	local res, err

	res, err = wg:wait(0.05)
	test:is( res, nil, 'failed after timeout' )
	test:is( err, 'Timed out', 'error after timeout' )
	test:is( completed, 0, 'no completed' )

	res, err = wg:wait(0.05)

	test:is( res, true, 'reveived success' )
	test:is( completed, N, 'all completed' )
	test:is( err, nil, 'no error' )
end, 3, 'recv -> send works')


test:done_testing()