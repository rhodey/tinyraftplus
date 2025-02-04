const { TinyRaftPlus, FsLog } = require('../index.js')

const sleep = (ms) => new Promise((res, rej) => setTimeout(res, ms))

function open(comms, a=1, b=null, opts={}, logFn=null) {
  b = b ? b : 1
  logFn = logFn ? logFn : (id) => new FsLog('/tmp/', `node-${id}`)
  const nodes = []
  for (let i = b; i <= a; i++) { nodes.push(i) }
  return nodes.map((id) => {
    const log = logFn(id)
    const send = (to, msg) => comms.send(to, id, msg)
    const node = new TinyRaftPlus(id, nodes, send, log, opts)
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

const start = (nodes) => Promise.all(nodes.map((node) => node.start()))

const stop = (nodes) => Promise.all(nodes.map((node) => node.stop()))

module.exports = {
  sleep, open, comms, start, stop,
}
