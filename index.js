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
  return new TinyRaft(opts)
}

module.exports = {
  open,
}
