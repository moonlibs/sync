package = 'sync'
version = 'scm-1'
source  = {
	url    = 'git+https://github.com/moonlibs/sync.git',
	branch = 'master',
}
description = {
	summary  = "Fiber synchronization primitives",
	homepage = 'https://github.com/moonlibs/sync.git',
	license  = 'BSD',
}
dependencies = {
	'lua >= 5.1'
}
build = {
	type = 'builtin',
	modules = {
		['sync'] = 'sync.lua';
		['sync.cond'] = 'sync/cond.lua';
		['sync.wg'] = 'sync/wg.lua';
		['sync.lock'] = 'sync/lock.lua';
		['sync.latch'] = 'sync/latch.lua';
		['sync.pool'] = 'sync/pool.lua';
	}
}

-- vim: syntax=lua
