local sync = {
	_VERSION = '0.9.0',
}

sync.cond  = require 'sync.cond'
sync.wg    = require 'sync.wg'
sync.cv    = sync.wg -- backward compatibility with old interface
sync.lock  = require 'sync.lock'
sync.latch = require 'sync.latch'

return sync
