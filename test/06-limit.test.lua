#!/usr/bin/env tarantool

local fiber = require 'fiber'
local test = require('test.ex').test('rate')

local sync = require('sync')
test:ok(sync.rate.new('rate', 1, 1), 'anon new by method')
test:ok(sync.rate(1, 1), 'anon new by call')

test:raises(function()
	sync.rate:new()
end, 'wrong constructor call')

test:raises(function()
	local rate = sync.rate()
	rate.wait()
end, 'Usage: rate:wait%(%) %(not rate%.wait%(%)%)', 'static call wait on object')
test:raises(function()
	local rate = sync.rate()
	rate.allow()
end, 'Usage: rate:allow%(%) %(not rate%.allow%(%)%)', 'static call allow on object')
test:raises(function()
	local rate = sync.rate()
	rate.reserve()
end, 'Usage: rate:reserve%(%) %(not rate%.reserve%(%)%)', 'static call reserve on object')

local function almost_gt(a, b, eps)
	if a > b then
		return true
	elseif a > b - eps then
		return true
	else
		return false
	end
end

test:deadline(function()
	local rate = sync.rate("rate", 3, 1) -- 3 rps / 1 at a time

	-- rate is full
	test:noyield(function()
		test:ok(rate:wait(), "first wait always return true")
	end)

	local s = fiber.time()
	test:ok(rate:wait(), "second token awaited")

	local elapsed = fiber.time()-s
	test:ok(elapsed >= 1/rate.rps, "second wait is above rps")

	s = fiber.time()
	for i = 1, 6 do
		test:ok(rate:wait(), "token " .. i .. " awaited")
	end
	local e = fiber.time() - s

	test:ok(almost_gt(e, 2, 0.01), "6 requests must take more than 2 seconds (rps=3)")
	test:ok(almost_gt(2.1, e, 0.01), "6 requests must take less than 2.1 seconds (rps=3)")
end, 3, "rate:wait(inf, 1)")

test:deadline(function()
	local rate = sync.rate("rate", 3, 1)

	test:noyield(function()
		test:ok(rate:wait(0.1), "token instantly awaited")
	end)

	test:noyield(function ()
		local ok, err = rate:wait(0.1)
		test:is(ok, false, "second token can't be available in 100ms (with rps=3)")
		test:is(err, "rate:wait(timeout=0.1, n=1) would exceed given timeout", "error message is correct")
	end)

	local s = fiber.time()
	test:ok(rate:wait(0.35), "token can be awaited in 350ms (rps=3)")
	local e = fiber.time()-s
	test:ok(almost_gt(e, 0.33, 0.01), "token was awaited in aproximately 330ms±10ms")
end, 3, "rate:wait(0.1, 1)")

test:deadline(function()
	local rate = sync.rate("rate", 3, 1)

	test:noyield(function()
		local ok, err = rate:wait(nil, 3) -- request more than burst
		test:is(ok, false, "can't be taken more than burst tokens")
		test:is(err, "rate:wait(timeout=inf, n=3) exceeds limiters burst=1", "correct error message")
	end)

	test:noyield(function()
		local ok, err = rate:allow(nil, 3) -- request more than burst
		test:is(ok, false, "can't be taken more than burst tokens")
		test:is(err, "not enough burst", "correct error message")
	end)

	test:noyield(function()
		local ok, err = rate:reserve(nil, 3) -- request more than burst
		test:is(ok, false, "can't be taken more than burst tokens")
		test:is(err, "not enough burst", "correct error message")
	end)

end, 3, "rate with n > burst")

test:deadline(function()
	local rate = sync.rate.new("rate", 3, 1)
	print(rate)
	test:ok(rate:allow(), "instant allow is okey") -- token was withdrawn

	test:noyield(function()
		local reserv = rate:reserve()
		assert(reserv)
		test:ok(reserv, "reservation is returned")

		test:ok(almost_gt(reserv.timeToAct, fiber.time()+1/rate.rps, 0.01), "timeToAct ≥ now+1/rps")
		test:ok(almost_gt(fiber.time()+2/rate.rps, reserv.timeToAct, 0.01), "timeToAct ≤ now+2/rps")
		reserv:cancel() -- cancel reservation

		reserv = rate:reserve()
		assert(reserv)
		test:ok(reserv, "reservation is returned")
		test:ok(almost_gt(reserv.timeToAct, fiber.time()+1/rate.rps, 0.01), "timeToAct ≥ now+1/rps")
		test:ok(almost_gt(fiber.time()+2/rate.rps, reserv.timeToAct, 0.01), "timeToAct ≤ now+2/rps")
	end)
end, 3, "instant allow and instant reservation")

test:deadline(function()
	local rate = sync.rate.new("rate", 0, 3)
	test:noyield(function()
		test:ok(rate:allow(), "token 1 - ok")
		test:ok(rate:allow(), "token 2 - ok")
		test:ok(rate:allow(), "token 3 - ok")

		local nok, err = rate:allow()
		test:ok(not nok, "token 4 - nok (rate was drained)")
		test:is(err, "not enough burst", "drained rate - not enough burst")
	end)
end, 3, "rps=0")

test:deadline(function()
	local rate = sync.rate.new("rate", 1/0)

	test:noyield(function()
		for _ = 1, 5 do
			test:ok(rate:allow(), "infinite rate almost allows")
		end
		for _ = 1, 5 do
			test:ok(rate:wait(), "infinite rate never waits")
		end
	end)

end, 3, "rps=inf")

test:done_testing()
