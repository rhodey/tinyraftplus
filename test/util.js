const { TinyRaftNode, TinyRaftLog } = require('../index.js')

function open(comms, a=1, b=null, opts={}) {
  b = b ? b : 1
  const nodes = []
  for (let i = b; i <= a; i++) { nodes.push(i) }
  return nodes.map((id) => {
    const log = new TinyRaftLog()
    const send = (to, msg) => comms.send(to, id, msg)
    const node = new TinyRaftNode(id, nodes, send, log, opts)
    comms.register(node)
    return node
  })
}

const sendAll = (to, from, msg) => true
const delayNone = (to, from, msg) => 0

function comms(allowSend=sendAll, delaySend=delayNone) {
  const nodes = []
  const register = (node) => nodes.push(node)

  async function send(to, from, msg) {
    if (!allowSend(to, from, msg)) { return }
    const delay = delaySend(to, from, msg)
    if (delay) { await sleep(delay) }
    const node = nodes.find((node) => node.nodeId === to)
    if (!node) { throw new Error(`node ${from} send to ${to} not found`) }
    node.onReceive(from, msg)
  }

  return { register, send }
}

const sleep = (ms) => new Promise((res, rej) => setTimeout(res, ms))

const start = (nodes) => Promise.all(nodes.map((node) => node.start()))

const stop = (nodes) => Promise.all(nodes.map((node) => node.stop()))

const leaders = (nodes) => nodes.filter((node) => node.state === 'leader')

const followers = (nodes) => nodes.filter((node) => node.state === 'follower')

module.exports = {
  open, comms, sleep, start, stop, leaders, followers
}
