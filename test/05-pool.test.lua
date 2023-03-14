#!/usr/bin/env tarantool

local fiber = require 'fiber'
local test = require('test.ex').test('pool')

local sync = require('sync')
test:ok(sync.pool.new(), 'anon new by method' )
test:ok(sync.pool(), 'anon new by call' )
test:ok(sync.pool.new('test1'), 'named new by method')
test:ok(sync.pool('test2'), 'named new by call')

test:raises(function ()
	sync.pool:new()
end, 'wrong constructor call')

test:raises(function ()
	local pool = sync.pool()
	pool.send()
end, 'Usage: pool:send%(%) %(not pool%.send%(%)%)', 'static call send on object')
test:raises(function ()
	local pool = sync.pool()
	pool.wait()
end, 'Usage: pool:wait%(%) %(not pool%.wait%(%)%)', 'static call wait on object')
test:raises(function ()
	local pool = sync.pool()
	pool.spawn()
end, 'Usage: pool:spawn%(%) %(not pool%.spawn%(%)%)', 'static call spawn on object')
test:raises(function ()
	local pool = sync.pool()
	pool.despawn()
end, 'Usage: pool:despawn%(%) %(not pool%.despawn%(%)%)', 'static call despawn on object')
test:raises(function ()
	local pool = sync.pool()
	pool.terminate()
end, 'Usage: pool:terminate%(%) %(not pool%.terminate%(%)%)', 'static call terminate on object')

test:deadline(function()
	local pool = sync.pool()
	local value = math.random()
	local task = pool:send(function()
		return value
	end)

	local ok, received = task:wait()
	test:ok(ok, 'task was successfully executed')
	test:is(received, value, 'received value equals sent value')
end, 3, 'send -> wait must return same value')

test:deadline(function()
	local pool = sync.pool()

	local t1_done = false
	local ft1 = fiber.time()
	pool:send(function()
		fiber.sleep(0.1)
		t1_done = true
	end)
	print("pool:send returned")

	local ft2 = fiber.time()
	test:ok(ft2 > ft1, "send yields fiber")
	test:ok(not t1_done, 'task still wasnt processed')

	test:ok(not pool:wait(0), 'pool has accepted given task')
	test:ok(pool:wait(), "pool has been successfully awaited")
	test:ok(t1_done, 'scheduled task was executed')

	pool:terminate()
	test:ok(pool.terminated, "pool has been terminated")
	test:ok(pool:wait(), "pool is awaited")

	fiber.sleep(0.15) -- termination lag is at least 100ms

	test:ok(pool.chan == nil, "pool has removed internal channel")
	test:is(require'fun'.length(pool.workers), 0, "all workers was removed")

end, 3, 'pool unlocks send after task was received on executor')

test:deadline(function()
	local pool = sync.pool(nil, 2)

	local jobs = {}
	local start = fiber.time()
	for i = 1, 3 do
		pool:send(function()
			jobs[i] = {registered = fiber.time()}
			fiber.sleep(0.5)
			jobs[i].processed = fiber.time()
		end)

		if i == 1 or i == 2 then
			test:ok(fiber.time() - start <= 0.005, "execution was returned almost instantly")
		elseif i == 3 then
			test:ok(fiber.time() - start >= 0.5, "3rd task has been waited until worker will become available")
		end
	end
end, 3, 'pool is limited')

test:deadline(function()
	local pool = sync.pool()

	local value1 = math.random()
	local value2 = math.random()

	local ok, ret1, ret2 = pool:send(function()
		return value1, value2
	end, {}, { async = false })

	test:ok(ok, "task executed successfully")
	test:is(ret1, value1, 'vararg matches - 1')
	test:is(ret2, value2, 'vararg matches - 2')

end, 3, 'task can be awaited in synchornouse way')

test:deadline(function()
	local pool = sync.pool()
	local value = math.random()
	local task = pool:send(function()
		fiber.sleep(0.1)
		return value
	end)

	local has_result, err = task:wait(0)
	test:is(has_result, nil, "result is nil - not given yet")
	test:is(err, "timed out", "error message is timed out")

	local ft = fiber.time()
	local ok, result = task:wait()
	local waited = fiber.time() - ft

	test:ok(ok, "result is awaited")
	test:is(result, value, "received result is the same as value")

	test:ok(waited <= 0.105, "result was awaited not for too long")

	test:noyield(function()
		ok, result = task:wait()
		test:ok(ok, "subsequent wait returns same result instantly")
		test:is(result, value, "returned result is constant")
	end)
end, 3, "result can be awaited")

test:deadline(function()
	local pool = sync.pool()

	local long = {}

	local long_running = pool:send(function()
		long.scheduled = true
		fiber.sleep(0.1)
		long.completed = true
	end)

	local short = {}
	local ret, err = pool:send(function()
		short.scheduled = true
		return 2
	end, {}, {wait_timeout = 0.01})

	test:ok(not ret, ":send returned false")
	test:is(err, "publish timed out (task was not scheduled)", "message was correct")

	test:is(long.completed, nil, "long task was not completed yet")
	test:ok(long_running:wait(0.5), "long task has been awaited")
	test:is(long.completed, true, "long task has been completed")

	fiber.sleep(0.2)
	test:is(short.scheduled, nil, "short task wasn't scheduled")
end, 3, "task can be cancelled before execution")

test:deadline(function()
	local pool = sync.pool(nil, 4)

	local results = {}
	local tasks = {}
	for i = 1, 4 do
		tasks[i] = pool:send(function()
			fiber.sleep(0.5 + math.random())
			results[i] = true
		end)
	end

	fiber.create(function()
		test:deadline(function()
			for i, task in ipairs(tasks) do
				test:ok(task:wait(), "task " .. i .. " has been for awaited")
				test:is(results[i], true, "result " .. i .. " was for processed")
			end
		end, 3, "all tasks must be awaited")
	end)

	pool:terminate()
	test:ok(pool.terminated, "pool has been terminated")

	test:raises(function()
		pool:send(function() fiber.sleep(1) end)
	end, 'pool:send%(%) attempt to schedule task on terminated pool')

	test:ok(pool:wait(2), "pool can be awaited after termination")

	test:ok(results[1], "task 1 was processed")
	test:ok(results[2], "task 2 was processed")
	test:ok(results[3], "task 3 was processed")
	test:ok(results[4], "task 4 was processed")

end, 3, "pool is continues processing all tasks when terminating")

test:done_testing()