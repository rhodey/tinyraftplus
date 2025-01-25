const test = require('tape')
const lib = require('../index.js')

const sleep = (ms) => new Promise((res, rej) => setTimeout(res, ms))

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

const start = (nodes) => nodes.forEach((node) => node.start())

const stop = (nodes) => nodes.forEach((node) => node.stop())

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

test('test elect n=3', async (t) => {
  t.plan(2)
  const coms = comms()
  const nodes = open(coms, 3)

  start(nodes)
  await sleep(100)

  const leaders = nodes.filter((node) => node.state === 'leader')
  t.equal(1, leaders.length, '1 leader')

  const followers = nodes.filter((node) => node.state === 'follower')
  t.equal(2, followers.length, '2 followers')

  let idk = leaders[0]
  console.log(123, idk)

  idk = followers[0]
  console.log(456, idk)

  t.teardown(() => stop(nodes))
})
