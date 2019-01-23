package = 'sync'
version = 'scm-1'
source  = {
	url    = 'git://github.com/moonlibs/sync.git',
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
	}
}

-- vim: syntax=lua
