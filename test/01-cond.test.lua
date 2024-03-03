#!/usr/bin/env tarantool

local fiber = require 'fiber'
local test = require('test.ex').test('cond')

local sync = require('sync')

test:ok(sync.cond.new(), 'anon new by method' )
test:ok(sync.cond(), 'anon new by call' )
test:ok(sync.cond.new('test1'), 'named new by method')
test:ok(sync.cond('test2'), 'named new by call')

test:raises(function ()
	sync.cond:new()
end, 'wrong constructor call')

test:raises(function ()
	local cond = sync.cond()
	cond.send()
end, 'Usage: cond:send%(value%) %(not cond%.send%(value%)%)', 'static call send on object')
test:raises(function ()
	local cond = sync.cond()
	cond.recv()
end, 'Usage: cond:send%(value%) %(not cond%.send%(value%)%)', 'static call send on object')

test:deadline(function()
	local cond = sync.cond()
	local value = math.random()
	cond:send(value)
	local received = cond:recv()
	test:is( received, value, 'reveived equals to sent' )
end, 3, 'send -> recv should not block')

test:deadline(function()
	local cond = sync.cond()
	local value = math.random()

	fiber.new(function ()
		fiber.sleep(0.1)
		cond:send(value)
	end)

	local received = cond:recv()

	test:is( received, value, 'reveived equals to sent' )
end, 3, 'recv -> send works')

test:deadline(function()
	local cond = sync.cond()
	local value = math.random()

	fiber.new(function ()
		fiber.sleep(0.1)
		cond:send(value)
	end)

	local recv, err = cond:recv(0.05)
	test:is( recv, nil, 'value not received after timeout' )
	test:is( err, 'Timed out', 'error after timeout' )

	local received = cond:recv()
	test:is( received, value, 'reveived equals to sent' )

end, 3, 'recv -> send works')

test:deadline(function()
	local cond = sync.cond()
	local value = math.random()

	test:noyield(function()
		local no_val, err = cond:recv(0)
		test:is(no_val, nil, 'empty cond returns no value')
		test:is(err, 'Timed out', 'error message is timed out')
	end)

	cond:send(value)

	test:noyield(function()
		local recv, err = cond:recv(0)
		test:is(recv, value, 'value received')
		test:is(err, nil, 'no error returned')
	end)

end, 1, 'recv noyields when timeout=0')

test:done_testing()