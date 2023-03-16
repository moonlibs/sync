local fiber = require 'fiber'
local log = require 'log'

---@class sync.pool
---@field name string
---@field workers Fiber[]
---@field chan fiber.channel
---@field wg sync.wg
---@field terminate_cb sync.cond signalled when pool is terminating
local pool = {}
pool.__index = pool
pool.__tostring = function (self) return "pool<".. (self.name or 'anon') ..">" end
setmetatable(pool, { __call = function (class, ...) return class.new(...) end })

local cond = require 'sync.cond'
local wg = require 'sync.wg'

local function pack(...)
	return { n = select('#', ...), ... }
end

local function tail_call(pcall_ok, ...)
	return pcall_ok, pack(...)
end

---Main worker loop
---@param t sync.pool.task
local function execute(t)
	t.scheduled_at = fiber.time()

	local pcall_ok, ret = tail_call(xpcall(t.func, debug.traceback, unpack(t.args)))

	t.resulted_at = fiber.time()
	t.result = ret
	t.is_error = pcall_ok == false

	if t.on_finish_cb then
		local ok, err
		if type(t.on_finish_ctx) ~= 'nil' then
			ok, err = pcall(t.on_finish_cb, t.on_finish_ctx, not t.is_error, unpack(t.result, 1, t.result.n))
		else
			ok, err = pcall(t.on_finish_cb, not t.is_error, unpack(t.result, 1, t.result.n))
		end
		if not ok then
			log.error("on_finish_cb failed: %s", err)
		end
	end

	-- notification
	if type(t.cb) == 'table' and not t.cb.sent then
		t.cb:send('processed')
	end
end

---Main worker loop
---@param p sync.pool
local function worker_main_loop(p)
	local self = fiber.self()
	local id = self:id()
	fiber.name(("pool/%s.%s"):format(p.name, id), {truncate = true})
	p.workers[id] = self

	---@return boolean
	local function should_live()
		fiber.testcancel()
		return p:_is_running() and p.workers[id] == self
	end

	log.info("Starting executor %s/%s", p.name, id)

	while should_live() do
		---@type sync.pool.task?
		local t
		repeat
			t = p.chan:get(0.01) --[[@as sync.pool.task?]]
		until (not should_live()) or t

		if type(t) ~= 'table' then
			goto continue
		end

		if t.cancelled then
			p.wg:done()
			goto continue
		end

		local ok, err = pcall(execute, t)
		p.wg:done()

		if not ok then
			log.error("execute on task %s failed: %s", t, err)

			if type(t) == 'table' then
				t.is_error = true
				t.result = { n = 1, ("executor failed: %s"):format(err) }
				if type(t.cb) == 'table' and not t.cb.sent then
					t.cb:send(true)
				end
			end
		end

		::continue::
	end

	if p.workers[id] == self then
		p.workers[id] = nil
	end
	log.info("Leaving executor %s", id)

	if p.terminated and not next(p.workers) then
		log.info("closing channel")
		p.chan:close()
		p.chan = nil
	end
end

---Creates new pool
---@param name string?
---@param pool_size number? amount of background executors (default=1)
---@return sync.pool
function pool.new(name, pool_size)
	if name == pool then error("Usage: pool.new([name]) or pool([name]) (not pool:new())", 2) end
	pool_size = tonumber(pool_size) or 1

	local fp = setmetatable({
		name = name or 'pool',
		chan = fiber.channel(),
		workers = {},
		wg = wg.new(),
		terminate_cb = cond.new(),
	}, pool)

	fp:spawn(pool_size)
	return fp
end


---@class sync.pool.runOpts
---@field async boolean run in async mode
---@field wait_timeout number timeout to wait for free executor in pool

---@class sync.pool.task
---@field terminate_cb sync.cond terminate cond is signalled when pool was terminated
---@field cb sync.cond condvar for synchornizations
---@field func fun() function to be executed on worker
---@field args { n: number, [any]: any } packed arguments of the call
---@field cancelled boolean? true when task was cancelled before execution
---@field result { n: number, [any]: any } packed result of the execution
---@field is_error boolean true when function raises execution (the error message will be in result[1])
---@field published_at number timestamp when task was published into channel
---@field scheduled_at number timestamp when worker took task from the channel
---@field resulted_at number timestamp when worker finished execution of the task
local task_mt = {}
task_mt.__index = task_mt

---Waits for result of the task and returns it to caller
---@param timeout number? timeout to wait for result (default: infinity)
---@param cancel_on_fail boolean? if true then closes cb if result wasn't awaited
---@return boolean|nil executed_ok, any ... # pcall_ok and vararg result
function task_mt:wait(timeout, cancel_on_fail)
	timeout = tonumber(timeout)

	if self.result then
		self.cb = nil
		self.pool = nil
		return not self.is_error, unpack(self.result, 1, self.result.n)
	end

	if self.cancelled then
		return nil, "task was cancelled"
	end

	local deadline
	if timeout then
		deadline = fiber.time() + tonumber(timeout)
	else
		deadline = math.huge
	end

	while fiber.time() < deadline and self.cb do
		fiber.testcancel()
		if self.result then
			self.cb = nil
			self.pool = nil
			return not self.is_error, unpack(self.result, 1, self.result.n)
		end
		self.cb:recv(math.min(1, deadline - fiber.time()))
	end

	if cancel_on_fail then
		if self.scheduled_at then
			log.warn("Task %s is already scheduled, cancel will do nothing", self)
		else
			self.cancelled = true
		end

		self.cb = nil
	end

	return nil, "timed out"
end


---On finish callback will be called at the end of the execution
---@generic T
---@param on_finish_cb fun(on_finish_ctx: T) callback
---@param on_finish_ctx T context will be passed as first argument to on_finish_cb
function task_mt:on_finish(on_finish_cb, on_finish_ctx)
	self.on_finish_ctx = on_finish_ctx
	self.on_finish_cb = on_finish_cb
end

function task_mt:__gc()
	self.terminate_cb = nil
	self.cb = nil
end

---Executes given function with arguments on pool
---@param func fun()|table function to execute
---@param args any[] arguments for the call will passed as arguments to given function
---@param opts sync.pool.runOpts options for the task (async is true by default)
function pool:send(func, args, opts)
	if getmetatable( self ) ~= pool then
		error("Usage: pool:send() (not pool.send())", 2)
	end
	if type(args) == 'nil' then
		args = {}
	end
	if type(args) ~= 'table' then error("Usage: pool:send(func, {args}, [{opts}]) arguments must be a list", 2) end

	if self.terminated then
		error("pool:send() attempt to schedule task on terminated pool", 2)
	end

	args.n = #args
	opts = opts or {}

	---@type sync.pool.task
	local task = setmetatable({
		func = func,
		args = args,
		cancelled = false,
		cb = cond.new(),
		terminate_cb = self.terminate_cb,
	}, task_mt)

	if opts.async ~= false then
		opts.async = true
	end

	local deadline
	if opts.wait_timeout then
		deadline = fiber.time() + opts.wait_timeout
		if deadline < fiber.time() then
			error("pool:send(): wait_timeout is too little", 2)
		end
	else
		deadline = math.huge
	end

	self.wg:start()

	while not self.terminate_cb:recv(0) and fiber.time() < deadline do
		fiber.testcancel()
		if self.chan:put(task, math.min(1, deadline-fiber.time())) then
			task.published_at = fiber.time()
			break
		end
	end

	if not task.published_at then
		task.cb = nil

		-- task is not in the channel, withdraw it
		self.wg:done()

		task.cancelled = true
		task.terminate_cb = nil

		return false, "publish timed out (task was not scheduled)"
	end

	if opts.async then
		return task
	end

	local wait_timeout
	if deadline ~= math.huge then
		wait_timeout = deadline - fiber.time()
	end
	return task:wait(wait_timeout, true)
end

---Spawns n more workers (by default 1)
---@param n number? how many workers needs to spawn
function pool:spawn(n)
	if getmetatable( self ) ~= pool then
		error("Usage: pool:spawn() (not pool.spawn())", 2)
	end

	n = tonumber(n) or 1

	for _ = 1, n do
		fiber.create(worker_main_loop, self)
	end
end

---Countes items in the table
---@param tbl table
local function count(tbl)
	local c = 0
	for _ in pairs(tbl) do
		c = c + 1
	end
	return c
end

---Despawns n workers (by default 1)
---@param n number? how many workers needs to despawn
function pool:despawn(n)
	if getmetatable( self ) ~= pool then
		error("Usage: pool:despawn() (not pool.despawn())", 2)
	end

	n = tonumber(n) or 1

	while n > 0 and count(self.workers) > 0 do
		local id2kill = next(self.workers)
		if self.workers[id2kill] then
			self.workers[id2kill] = nil
			n = n - 1
		end
	end
end

---Checks that pool is running
---@return boolean
function pool:_is_running()
	return self.terminated ~= true
end

---Terminates pool
---@param force boolean? if true then closes channel and cancels workers
function pool:terminate(force)
	if getmetatable( self ) ~= pool then
		error("Usage: pool:terminate() (not pool.terminate())", 2)
	end

	if not self.terminated then
		self.terminated = true
		if type(self.terminate_cb) == 'table' and not self.terminate_cb.sent then
			self.terminate_cb:send(true)
		end
		if not force then return end
	end

	self.chan:close()
	self.chan = nil

	for id, w in pairs(self.workers) do
		if type(w) == 'table' and w.cancel and w:status() ~= "dead" then
			local ok, err = pcall(w.cancel, w)
			if not ok then
				log.error("failed to cancel fiber %s: %s", w, err)
			else
				self.workers[id] = nil
			end
		end
	end
end

---awaits when all started job will finish for timeout
---@param timeout number? timeout in seconds (default inifinity)
---@return true|nil awaited, string? error_message
function pool:wait(timeout)
	if getmetatable( self ) ~= pool then
		error("Usage: pool:wait() (not pool.wait())", 2)
	end

	return self.wg:wait(timeout)
end

return pool