const test = require('tape')
const lib = require('../index.js')

function open(comms, n=1) {
  const nodes = []
  for (let i = 1; i <= n; i++) { nodes.push(i) }
  return nodes.map((id) => {
    const send = (to, msg) => comms.send(to, id, msg)
    const node = lib.open(id, nodes, send)
    comms.register(node)
    return node
  })
}

function comms() {
  const nodes = []
  const register = (node) => nodes.push(node)

  function send(to, from, msg) {
    const node = nodes.find((node) => node.nodeId === to)
    if (!node) { throw new Error(`node ${from} send to ${to} not found`) }
    node.onReceive(from, msg)
  }

  return { register, send }
}

const sleep = (ms) => new Promise((res, rej) => setTimeout(res, ms))

const start = (nodes) => nodes.forEach((node) => node.start())

const stop = (nodes) => nodes.forEach((node) => node.stop())

const leaders = (nodes) => nodes.filter((node) => node.state === 'leader')

const followers = (nodes) => nodes.filter((node) => node.state === 'follower')

test('test elect n=3', async (t) => {
  t.plan(2)
  const coms = comms()
  const nodes = open(coms, 3)

  start(nodes)
  await sleep(100)

  let arr = leaders(nodes)
  t.equal(1, arr.length, '1 leader')

  let debug = arr[0]
  console.log(123, debug)

  arr = followers(nodes)
  t.equal(2, arr.length, '2 followers')

  debug = arr[0]
  console.log(456, debug)

  t.teardown(() => stop(nodes))
})
