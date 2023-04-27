local fiber = require 'fiber'
local tap = require 'tap'

if os.getenv("LUACOV_ENABLE") then
    print("enabling luacov")
    require 'luacov.runner'.init()
end

local yaml = require 'yaml'.new()
yaml.cfg {
    encode_use_tostring = true,
}

local function upvalues(f)
    local info = debug.getinfo(f, "uS")
    local vars = {}

    if (info and info.what == "Lua") then
        for i = 1, info.nups do
            local key, value = debug.getupvalue(f, i)
            vars[key] = value
        end
    end

    return vars
end

local test_mt = assert(upvalues(upvalues(tap.test).test).test_mt, "Can't find test metatable").__index

function test_mt.raises(test, f, ...)
    local like, message
    if select('#', ...) >= 2 then
        like, message = ...
    else
        message = ...
    end
    test.total = test.total + 1
    local r, err = pcall(f)
    if not r then
        if not like or string.match(tostring(err), like) ~= nil then
            io.write(string.format("ok - %s (has exception)\n", message))
            return true
        else
            io.write(string.format("not ok - %s (no exception)\n", message))
            for line in yaml.encode({
                got = err,
                expected = like,
            }):gmatch("[^\n]+") do
                io.write('#', string.rep(' ', 2 + 4 * test.level), line, "\n")
            end
            return false
        end
    else
        io.write(string.format("not ok - %s (no exception)\n", message))
        return false
    end
end

function test_mt.done_testing(test, num_tests)
    local plan = test.planned
    local count = test.total

    if num_tests then
        if plan == 0 then
            test:plan(num_tests)
        end
    elseif count > 0 and num_tests and count ~= num_tests then
        io.write(string.format("not ok - planned to run %d but done_testing() expects %d\n", num_tests, count))
        test.failed = test.failed + 1
    else
        num_tests = count
    end
    if plan ~= 0 and plan ~= num_tests then
        io.write(string.format("not ok - planned to run %d but done_testing() expects %d\n", plan, num_tests))
        test.failed = test.failed + 1
    end
    if plan == 0 then
        test:plan(num_tests)
    end

    if test.failed > 0 then
        os.exit(1)
    else
        os.exit(0)
    end
end

local function get_line_name(level)
    level = 1 + (level or 2)
    local info = debug.getinfo(level, "nSl")
    if not info then return 'unknown' end
    return info.short_src .. ':' .. info.currentline;
end

function test_mt.deadline(test, f, timeout, message)
    test.total = test.total + 1
    timeout = timeout or 5
    message = message or get_line_name(2)
    local success = false
    local diag
    local cond = fiber.cond()
    local watchee = fiber.new(function()
        local r, e = pcall(f)
        cond:broadcast()
        if r then
            success = true
        else
            diag = diag or string.format("Failed with error: %s", e)
        end
    end)
    if not cond:wait(timeout) then
        diag = string.format("Timed out")
        fiber.kill(watchee)
    end
    if not success then
        io.write(string.format("not ok - %s (deadline)\n", message))
        if diag then
            io.write('#', string.rep(' ', 2 + 4 * test.level), diag, "\n")
        end
        test.failed = test.failed + 1
        return false
    else
        io.write(string.format("ok - %s (deadline)\n", message))
        return true
    end
end

function test_mt.noyield(test, f, message)
    test.total = test.total + 1
    message = message or get_line_name(2)
    local success = false
    local diag

    local time = fiber.time64()
    local r, e = pcall(f)
    local yielded_for = fiber.time64() - time
    if r then
        success = true
    else
        diag = diag or string.format("Failed with error: %s", e)
    end

    if yielded_for == 0 and success then
        io.write(string.format("ok - %s (noyield)\n", message))
        return true
    else
        test.failed = test.failed + 1
        io.write(string.format("no ok - %s (noyield)\n", message))
        if yielded_for ~= 0 then
            io.write('#', string.rep(' ', 2 + 4 * test.level),
                ("yielded for %0.6fs\n"):format(tonumber(yielded_for) / 1e6))
        end
        if not success then
            io.write('#', string.rep(' ', 2 + 4 * test.level), diag, "\n")
        end
        return false
    end
end

return tap
