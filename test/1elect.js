const test = require('tape')
const lib = require('../index.js')
const { open, comms, sleep, start, stop, leaders, followers } = require('./util.js')

// const { nodeId, nodes, state, leader, followers, term } = node

test('test elect n=3', async (t) => {
  t.plan(3)
  const coms = comms()
  const nodes = open(coms, 3)
  start(nodes)
  await sleep(100)

  const ok = nodes.every((node) => [1, 2, 3].includes(node.nodeId))
  t.ok(ok, 'ids correct')

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')

  arr = followers(nodes)
  t.equal(arr.length, 2, '2 followers')

  t.teardown(() => coms.close())
})

test('test elect n=3 and 1 offline', async (t) => {
  t.plan(4)
  const allowSend = (to, from, msg) => from !== 3
  const coms = comms(allowSend)
  const nodes = open(coms, 3)
  start(nodes)
  await sleep(100)

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')
  t.ok(arr[0].nodeId !== 3, 'leader not node 3')

  arr = followers(nodes)
  t.equal(arr.length, 1, '1 follower')
  t.ok(arr[0].nodeId !== 3, 'follower not node 3')

  t.teardown(() => coms.close())
})

test('test elect n=5', async (t) => {
  t.plan(2)
  const coms = comms()
  const nodes = open(coms, 5)
  start(nodes)
  await sleep(100)

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')

  arr = followers(nodes)
  t.equal(arr.length, 4, '4 followers')

  t.teardown(() => coms.close())
})

test('test elect n=5 and 1 offline', async (t) => {
  t.plan(4)
  const allowSend = (to, from, msg) => from !== 5
  const coms = comms(allowSend)
  const nodes = open(coms, 5)
  start(nodes)
  await sleep(100)

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')
  t.ok(arr[0].nodeId !== 5, 'leader not node 5')

  arr = followers(nodes)
  t.equal(arr.length, 3, '3 followers')
  const ok = arr.every((node) => node.nodeId !== 5)
  t.ok(ok, 'followers not node 5')

  t.teardown(() => coms.close())
})

test('test elect n=5 and 1 delayed', async (t) => {
  t.plan(5)
  const delaySend = (to, from, msg) => from === 5 ? 1_000 : 0
  const coms = comms(undefined, delaySend)
  const nodes = open(coms, 5)
  start(nodes)

  await sleep(100)
  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')

  arr = followers(nodes)
  t.equal(arr.length, 3, '3 followers')
  const ok = arr.every((node) => node.nodeId !== 5)
  t.ok(ok, 'followers not node 5')

  await sleep(2_500)
  arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')

  arr = followers(nodes)
  t.equal(arr.length, 4, '4 followers')

  t.teardown(() => coms.close())
})

test('test elect n=5 and 2 offline', async (t) => {
  t.plan(6)
  const allowSend = (to, from, msg) => from !== 4 && from !== 5
  const coms = comms(allowSend)
  const nodes = open(coms, 5)
  start(nodes)
  await sleep(100)

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')
  t.ok(arr[0].nodeId !== 4, 'leader not node 4')
  t.ok(arr[0].nodeId !== 5, 'leader not node 5')

  arr = followers(nodes)
  t.equal(arr.length, 2, '2 followers')
  let ok = arr.every((node) => node.nodeId !== 4)
  t.ok(ok, 'followers not node 4')
  ok = arr.every((node) => node.nodeId !== 5)
  t.ok(ok, 'followers not node 5')

  t.teardown(() => coms.close())
})

test('test elect n=5 and 2 delayed', async (t) => {
  const delaySend = (to, from, msg) => (from === 4 || from === 5) ? 1_000 : 0
  const coms = comms(undefined, delaySend)
  const nodes = open(coms, 5)
  start(nodes)

  await sleep(100)
  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')

  arr = followers(nodes)
  t.equal(arr.length, 2, '2 followers')
  let ok = arr.every((node) => node.nodeId !== 4)
  t.ok(ok, 'followers not node 4')
  ok = arr.every((node) => node.nodeId !== 5)
  t.ok(ok, 'followers not node 5')

  let total = 0

  while (total < 15_000) {
    await sleep(100)
    total += 100

    arr = leaders(nodes)
    if (arr.length > 1) {
      t.equal(arr.length, 1, '1 leader')
    }

    arr = followers(nodes)
    if (arr.length > 4) {
      t.equal(arr.length, 4, '4 followers')
    } else if (arr.length === 4) {
      t.equal(arr.length, 4, '4 followers')
      break
    }
  }

  t.teardown(() => coms.close())
})
