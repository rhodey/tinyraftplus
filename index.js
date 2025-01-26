const TinyRaft = require('tinyraft')

const defaults = {
  heartbeatTimeout: 500,
  electionTimeout: 2_500,
  leadershipTimeout: 5_000,
  initialTerm: 0, leaderPriority: 0,
}

function open(nodeId, nodes, send, opts={}) {
  opts = { ...opts, nodeId, nodes, send }
  opts = { ...defaults, ...opts }
  return new TinyRaftLog(opts)
}

class TinyRaftLog extends TinyRaft {
  constructor(config) {
    super(config)
    this.stopped = false
  }

  start() {
    if (this.stopped) { throw new Error('stopped') }
    super.start()
  }

  stop() {
    this.stopped = true
    super.stop()
  }

  markAlive() {
    if (this.stopped) { return }
    super.markAlive()
  }

  onReceive(from, msg) {
    if (this.stopped) { return }
    super.onReceive(from, msg)
  }
}

module.exports = {
  open,
}
