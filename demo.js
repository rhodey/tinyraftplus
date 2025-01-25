const TinyRaft = require('tinyraft')

const defaults = {
  electionTimeout: 5000,
  heartbeatTimeout: 1000,
  leadershipTimeout: 10000,
  initialTerm: 0, leaderPriority: 0,
}

const sleep = (ms) => new Promise((res, rej) => setTimeout(res, ms))

function onError(err) {
  console.error('error', err)
  process.exit(1)
}

function send(to, msg) {
  console.log(to, msg)
}

function open(nodeId, nodes, opts={}) {
  opts = { ...opts, nodeId, nodes, send }
  opts = { ...defaults, ...opts }
  return new TinyRaft(opts)
}

async function main() {
  let nodes = [1, 2, 3]
  nodes = nodes.map((id) => open(id, nodes))

  nodes.forEach((node) => {
    const id = node.nodeId
    node.on('change', (state) => console.log(id, 'change', state))
    node.start()
  })

  await sleep(2_500)
  nodes.forEach((node) => node.stop())
}

main().catch(onError)
