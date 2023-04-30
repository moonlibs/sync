#!/usr/bin/env tarantool

local fiber = require 'fiber'
local test = require('test.ex').test('pool')

local sync = require('sync')
test:ok(sync.pool.new(), 'anon new by method')
test:ok(sync.pool(), 'anon new by call')
test:ok(sync.pool.new('test1'), 'named new by method')
test:ok(sync.pool('test2'), 'named new by call')

test:raises(function()
	sync.pool:new()
end, 'wrong constructor call')

test:raises(function()
	local pool = sync.pool()
	pool.send()
end, 'Usage: pool:send%(%) %(not pool%.send%(%)%)', 'static call send on object')
test:raises(function()
	local pool = sync.pool()
	pool.wait()
end, 'Usage: pool:wait%(%) %(not pool%.wait%(%)%)', 'static call wait on object')
test:raises(function()
	local pool = sync.pool()
	pool.spawn()
end, 'Usage: pool:spawn%(%) %(not pool%.spawn%(%)%)', 'static call spawn on object')
test:raises(function()
	local pool = sync.pool()
	pool.despawn()
end, 'Usage: pool:despawn%(%) %(not pool%.despawn%(%)%)', 'static call despawn on object')
test:raises(function()
	local pool = sync.pool()
	pool.terminate()
end, 'Usage: pool:terminate%(%) %(not pool%.terminate%(%)%)', 'static call terminate on object')

sync.pool.debug = true

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
	test:is(require 'fun'.length(pool.workers), 0, "all workers was removed")
end, 3, 'pool unlocks send after task was received on executor')

test:deadline(function()
	local pool = sync.pool(nil, 2)

	local jobs = {}
	local start = fiber.time()
	for i = 1, 3 do
		pool:send(function()
			jobs[i] = { registered = fiber.time() }
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
end, 3, 'task can be awaited in  way')

test:deadline(function()
	local pool = sync.pool()
	local value = math.random()
	local task = pool:send(function()
		fiber.sleep(0.1)
		return value
	end)

	local has_result, err = task:wait(0)
	test:is(has_result, nil, "result is nil - not given yet")
	test:is(err, "task await timed out", "error message is task await timed out")

	local ft = fiber.time()
	local ok, result = task:wait()
	local waited = fiber.time() - ft

	test:ok(ok, "result is awaited")
	test:is(result, value, "received result is the same as value")

	test:ok(waited <= 0.2, "result was awaited not for too long")

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
	end, {}, { wait_timeout = 0.01 })

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

test:deadline(function()
	local pool = sync.pool.new('pool', 3)

	local termchan = fiber.channel()
	local final = fiber.channel()
	fiber.create(function()
		-- do some real work
		test:raises(function()
			for i = 1, 10 do
				local t = pool:send(function()
					fiber.sleep(i)
					fiber.testcancel()
				end)
				if t then
					t:on_finish(function(ok, ...)
						print("on_finish", ok, ...)
					end)
				end
				if i % 4 == 0 then
					termchan:put(true)
				end
			end
		end, 'pool:send%(%) attempt to schedule task on terminated pool', 'schedule on forcefully terminated pool')
		final:put(true)
	end)

	termchan:get()
	pool:terminate(true)
	assert(pool:wait())
	final:get()
end, 3, "moonlibs/sync/issues/4")

test:deadline(function()
	local pool = sync.pool.new('pool', 3)

	local fin = fiber.channel()
	pool:send(fiber.sleep, { 0.1 }):on_finish(fin.put, fin)
	pool:send(fiber.sleep, { 0.1 }):on_finish(fin.put, fin)
	pool:send(fiber.sleep, { 0.1 }):on_finish(fin.put, fin)

	local was_executed = false
	local ok, err = pool:send(function() was_executed = true end, {}, { wait_timeout = 0.001 })

	test:is(ok, nil, "last task was not finished")
	test:is(err, sync.pool.errors.TASK_WAS_NOT_SCHEDULED, "error message is task was not schedulled")
	test:is(was_executed, false, "not scheduled task must not be executed")

	for _ = 1, 3 do fin:get() end
	test:is(was_executed, false, "even when pool is empty not schedulled task must not be executed")

	pool:terminate()
end, 3, "synchronous pool with wait_timeout")

test:deadline(function()
	local pool = sync.pool.new('pool', 1)
	pool:send(fiber.sleep, { 1 })

	test:raises(function()
		pool:send(function()
		end, {}, { wait_timeout = -0.1 })
	end, 'pool:send%(%): wait_timeout is too little')
end, 1, "synchronous send with maformed wait_timeout")

test:deadline(function()
	local pool = sync.pool.new('pool', 1)
	local ok, err = pool:send(fiber.sleep, { 0.5 }, { wait_timeout = 0.05, async = false })

	test:is(ok, nil, "task cant be awaited")
	test:is(err, sync.pool.errors.TASK_WAS_NOT_AWAITED, "error message should be task was not awaited")

	pool:terminate()
	pool:wait()
end, 1, "synchornous long running task with little wait_timeout can not be awaited")

test:deadline(function()
	local pool = sync.pool.new('pool', 1)
	local task = pool:send(fiber.sleep, { 0.1 })
	assert(type(task) == 'table')

	local fin = fiber.channel()
	task:on_finish(function(t)
		t.cb = {}
		fin:put(true)
	end, task)

	fin:get()
	fiber.yield()

	local ok, res = task:wait()
	test:is(ok, false, "corrupted task is failed")
	test:is(task.is_error, true, "task finished with error")

	pool:terminate()
	pool:wait()
end, 1, "corrupted internal task structure handled correctly")

test:deadline(function()
	local pool = sync.pool.new('pool', 1)

	local fins = {}
	pool:send(fiber.sleep, { 0.1 }):on_finish(function()
		fins[1] = true
	end)

	pool:spawn(1)
	test:is(require 'fun'.length(pool.workers), 2, "now pool has 2 workers")

	pool:send(fiber.sleep, { 0.1 }):on_finish(function()
		fins[2] = true
	end)

	pool:despawn(1)
	pool:wait()
	pool:terminate()

	test:is(fins[1], true, "first task finished ok")
	test:is(fins[2], true, "second task finished ok")
end, 1, "spawn/despawn does not cancel scheduled tasks")

test:deadline(function()
	local pool = sync.pool.new('pool', 2)
	pool:send(function()
		fiber.sleep(0.1)
	end)

	local executed = false
	local task = assert(pool:send(function(...) executed = true fiber.sleep(...) end, {1}))
	test:ok(task, "task must be returned")
	test:is(task.scheduled_at, nil, "task must not be scheduled yet")
	local err, res = task:wait(0, true)

	test:is(err, nil, "task:wait(0, true) must return nil")
	test:is(task.cancelled, true, "task must be cancelled")
	test:is(res, "task await timed out", "task:wait must return await timed out")

	pool:terminate()
	pool:wait()
	test:is(task.scheduled_at, nil, "task must not be scheduled")
	fiber.sleep(0.1)
	test:is(executed, false, "task must not be executed")

	local ft = fiber.time()
	local err2, res2 = task:wait(1)
	local waited = fiber.time()-ft
	test:is(err2, nil, "double task wait on cancelled")
	test:is(res2, "task was cancelled", "second wait must return task was cancelled")
	test:is(waited, 0, "task:wait on cancelled task must return instantly")
end, 1, "task autocancelled when wait_timeout is too low")

test:done_testing()
