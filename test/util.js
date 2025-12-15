const { FsLog } = require('../src/index.js')
const { RaftNode } = require('../src/index.js')

const noop = () => {}

const sleep = (ms) => new Promise((res, rej) => setTimeout(res, ms))

const sendAll = (to, from, msg) => true

const delayNone = (to, from, msg) => 0

function comms(allowSend=sendAll, delaySend=delayNone) {
  const nodes = []
  const register = (node) => nodes.push(node)

  async function send(to, from, msg) {
    if (!allowSend(to, from, msg)) { return }
    const delay = delaySend(to, from, msg)
    if (delay) { await sleep(delay) }
    const node = nodes.find((node) => node.id === to)
    if (!node) { throw new Error(`node ${from} send to ${to} not found`) }
    node.onReceive(from, msg)
  }

  return { register, send }
}

function connect(comms, a=1, b=null, opts={}, logFn=null) {
  b = b ? b : 1
  logFn = logFn ?? ((id) => new FsLog('/tmp/', `node-${id}`))
  const nodes = []
  for (let i = b; i <= a; i++) { nodes.push(i) }
  return nodes.map((id) => {
    const log = logFn(id)
    const send = (to, msg) => comms.send(to, id, msg)
    const node = new RaftNode(id, nodes, send, log, opts)
    comms.register(node)
    return node
  })
}

const open = (nodes) => Promise.all(nodes.map((node) => node.open()))

const close = (nodes) => Promise.all(nodes.map((node) => node.close()))

const awaitResolve = (promises, minimum) => {
  return new Promise((res, rej) => {
    let c = 0
    promises.forEach((promise) => {
      promise.then(() => {
        if (++c < minimum) { return }
        res()
      }).catch(noop)
    })
  })
}

const ready = (nodes, count=null, commit=false) => {
  count = count ?? nodes.length
  return awaitResolve(nodes.map((node) => node.awaitLeader(commit)), count)
}

const leaders = (nodes) => nodes.filter((node) => node.state === 'leader')

const followers = (nodes) => nodes.filter((node) => node.state === 'follower')

module.exports = {
  sleep, comms, connect,
  open, close, ready,
  leaders, followers,
}
