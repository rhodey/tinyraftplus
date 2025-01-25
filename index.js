const TinyRaft = require('tinyraft')

const defaults = {
  electionTimeout: 5000,
  heartbeatTimeout: 1000,
  leadershipTimeout: 10000,
  initialTerm: 0, leaderPriority: 0,
}

function open(nodeId, nodes, send, opts={}) {
  opts = { ...opts, nodeId, nodes, send }
  opts = { ...defaults, ...opts }
  return new TinyRaft(opts)
}

module.exports = {
  open,
}
