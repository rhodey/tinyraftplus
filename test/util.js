const lib = require('../index.js')

function open(comms, n=1, b=null, opts={}) {
  b = b ? b : 1
  const nodes = []
  for (let i = b; i <= n; i++) { nodes.push(i) }
  return nodes.map((id) => {
    const send = (to, msg) => comms.send(to, id, msg)
    const node = lib.open(id, nodes, send, opts)
    comms.register(node)
    return node
  })
}

const sendAll = (to, from, msg) => true
const delayNone = (to, from, msg) => 0

function comms(allowSend=sendAll, delaySend=delayNone) {
  let nodes = []
  const register = (node) => nodes.push(node)

  function close() {
    stop(nodes)
    nodes = null
  }

  async function send(to, from, msg) {
    if (!allowSend(to, from, msg)) { return }
    const delay = delaySend(to, from, msg)
    if (delay) { await sleep(delay) }
    if (!nodes) { return }
    const node = nodes.find((node) => node.nodeId === to)
    if (!node) { throw new Error(`node ${from} send to ${to} not found`) }
    node.onReceive(from, msg)
  }

  return { register, send, close }
}

const sleep = (ms) => new Promise((res, rej) => setTimeout(res, ms))

const start = (nodes) => nodes.forEach((node) => node.start())

const stop = (nodes) => nodes.forEach((node) => node.stop())

const leaders = (nodes) => nodes.filter((node) => node.state === 'leader')

const followers = (nodes) => nodes.filter((node) => node.state === 'follower')

module.exports = {
  open, comms, sleep, start, stop, leaders, followers
}
