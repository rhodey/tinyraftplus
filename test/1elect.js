const test = require('tape')
const lib = require('../index.js')
const { open, comms, sleep, start, stop, leaders, followers } = require('./util.js')

test('test elect n=3', async (t) => {
  t.plan(2)
  const coms = comms()
  const nodes = open(coms, 3)
  start(nodes)
  await sleep(100)

  let arr = leaders(nodes)
  t.equal(1, arr.length, '1 leader')

  arr = followers(nodes)
  t.equal(2, arr.length, '2 followers')

  /*
  const node = arr[0]
  const { nodeId, nodes, state, leader, followers, term } = node
  */

  t.teardown(() => stop(nodes))
})

test('test elect n=3 and 1 offline', async (t) => {
  t.plan(6)
  const allowSend = (to, from, msg) => from !== 3
  const coms = comms(allowSend)
  const nodes = open(coms, 3)
  start(nodes)
  await sleep(100)

  let arr = leaders(nodes)
  t.equal(1, arr.length, '1 leader')
  t.ok(typeof arr[0].nodeId === 'number', 'leader id is number')
  t.ok(arr[0].nodeId !== 3, 'leader is not node 3')

  arr = followers(nodes)
  t.equal(1, arr.length, '1 follower')
  t.ok(typeof arr[0].nodeId === 'number', 'follower id is number')
  t.ok(arr[0].nodeId !== 3, 'follower is not node 3')

  t.teardown(() => stop(nodes))
})
