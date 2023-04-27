[![Coverage Status](https://coveralls.io/repos/github/moonlibs/sync/badge.svg?branch=master)](https://coveralls.io/github/moonlibs/sync?branch=master)

# Collection of synchronization primitives for Tarantool fibers

## Conditional Variable (cond)

Current implementation of Tarantool's `fiber.cond` has unexpected behavior if signal/broadcast was executed before wait (unlike other implementations of conditional variable). So, with `sync.cond` the following code will not freeze forever.

And unlike fiber cond (which are, in fact, more like signals) it is one time use only.

```lua
local cond = sync.cond()

cond:send(data)
local value = cond:recv()
```

```lua
local cond = sync.cond()

fiber.create(function()
	fiber.sleep(1)
	cond:send('some data')
end)

print(cond:recv())
```

## WaitGroup (wg)

Used to wait for finishing of several simultaneous/parallel tasks.

```lua
local wg = sync.wg()

for 1..10 do
	wg:start() -- or wg:begin()
	fiber.create(function()
		wg:done() -- or wg:finish()
	end)
end

wg:wait(timeout)
```

There is a pair of methods: `start` & `finish` with a synonims `add` + `done` for easy migration from other languages (golang).
Sadly we cannod use the pair `begin`/`end`, since `end` is a keyword. One could use mixed combination: `start` + `done` or `add` + `finish`

`start` also supports number (like add in Go), but that's not recommended.

There is alternative name `sync.cv` for `sync.wg` for compatibility with the previous version.


## Mutex (lock) with deadlock detection

Heavyweight mutex, which is assigned to fiber. That allows to implement deadlock detection.

```lua
local lock = sync.lock()

for i = 1, 3 do
	fiber.create(function(i)
		lock:acquire()
		fiber.sleep(math.random())
		print(i, "doing work")
		fiber.sleep(math.random())
		lock:release()
	end,i)
end

lock:with(function()
	-- critical section
end)
```

## Latch (lightweight lock)

Binding to tarantool's builtin latch: Latch of cooperative multitasking environment, which preserves strict order of fibers waiting for the latch.

Rather performant, but without any sugar, like deadlock detection

```lua
local lock = sync.latch()

for i = 1, 3 do
	fiber.create(function(i)
		lock:acquire()
		fiber.sleep(math.random())
		print(i, "doing work")
		fiber.sleep(math.random())
		lock:release()
	end,i)
end
```

## Pool (fiber pool)

Implementation of fire-and-forget fiber pool.

```lua
local http = require 'http.client'
local pool = sync.pool('workers', 4)

for i = 1, 16 do
	pool:send(function(url)
		local r = http.get(url)
		assert(r.status == 200)
		return r.status, r.headers, r.body
	end, {"https://tarantool.io"})
end

pool:wait() -- pool can be awaited
print("pool finished")
```

sync.pool is usefull in background fibers when you need to parallel networks requests
```lua

function job:start()
    self.fiber_f = fiber.create(function()
        local pool = sync.pool('fetches', 4)
        while self.is_active do
            for _, user in box.space.users:pairs() do
                if self.is_active then
                    -- fast exit
                    break
                end
                pool:send(process_user_network, {user})
            end
        end
        pool:terminate()
        if not pool:wait(gracefull_timeout) then
            -- terminate pool with force
            log.warn("forcefully terminating pool")
            pool:terminate(true)
            pool:wait()
        end
    end)
end

```

## More plans and ideas to implement

There are several ideas may be implemented. PR's or proposals are welcome

* Named wait groups — names instead of counters
* fiber.select — ability to wait for something waitable (like in go)
	* fiber.select or wait_any/wait_all
	* https://github.com/tarantool/tarantool/issues/5635
* Joinable fiber pool
* "Normal" joinable fiber (like coro)
	* able to "return"
	* able to rethrow
	* zombie status: no tombstones in fiber pool
* Channel+luafun
