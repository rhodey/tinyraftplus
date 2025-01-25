const TinyRaft = require('tinyraft')

const defaults = {
  electionTimeout: 2500,
  heartbeatTimeout: 500,
  leadershipTimeout: 5000,
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
