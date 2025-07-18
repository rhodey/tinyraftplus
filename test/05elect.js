const test = require('tape')
const { sleep, connect, comms, open, close, ready } = require('./util.js')

// const { nodeId, nodes, state, leader, followers, term } = node

const leaders = (nodes) => nodes.filter((node) => node.state === 'leader')

const followers = (nodes) => nodes.filter((node) => node.state === 'follower')

test('test elect n=3', async (t) => {
  t.plan(5)
  t.teardown(() => close(nodes))

  const coms = comms()
  const nodes = connect(coms, 3)
  await open(nodes)
  await ready(nodes)

  let ok = nodes.every((node) => [1, 2, 3].includes(node.nodeId))
  t.ok(ok, 'ids correct')
  ok = nodes.every((node) => node.isOpen)
  t.ok(ok, 'nodes are open')

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')

  const count = arr[0]?.followers?.length - 1
  arr = followers(nodes)
  t.equal(arr.length, 2, '2 followers')
  t.equal(count, 2, '2 followers again')
})

test('test elect n=3 and 1 offline', async (t) => {
  t.plan(5)
  t.teardown(() => close(nodes))

  const allowSend = (to, from, msg) => from !== 3
  const coms = comms(allowSend)
  const nodes = connect(coms, 3)
  await open(nodes)
  await ready(nodes, 2)

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')
  t.ok(arr[0].nodeId !== 3, 'leader not node 3')

  const count = arr[0]?.followers?.length - 1
  arr = followers(nodes)

  t.equal(arr.length, 1, '1 follower')
  t.equal(count, 1, '1 follower again')
  t.ok(arr[0].nodeId !== 3, 'follower not node 3')
})

test('test elect n=5', async (t) => {
  t.plan(3)
  t.teardown(() => close(nodes))

  const coms = comms()
  const nodes = connect(coms, 5)
  await open(nodes)
  await ready(nodes)

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')

  const count = arr[0]?.followers?.length - 1
  arr = followers(nodes)
  t.equal(arr.length, 4, '4 followers')
  t.equal(count, 4, '4 followers again')
})

test('test elect n=5 and 1 offline', async (t) => {
  t.plan(5)
  t.teardown(() => close(nodes))

  const allowSend = (to, from, msg) => from !== 5
  const coms = comms(allowSend)
  const nodes = connect(coms, 5)
  await open(nodes)
  await ready(nodes, 4)

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')
  t.ok(arr[0].nodeId !== 5, 'leader not node 5')

  const count = arr[0]?.followers?.length - 1
  arr = followers(nodes)
  t.equal(arr.length, 3, '3 followers')
  t.equal(count, 3, '3 followers again')

  const ok = arr.every((node) => node.nodeId !== 5)
  t.ok(ok, 'followers not node 5')
})

test('test elect n=5 and 1 delayed', async (t) => {
  t.teardown(() => close(nodes))

  const delaySend = (to, from, msg) => from === 5 ? 1_000 : 0
  const coms = comms(undefined, delaySend)
  const nodes = connect(coms, 5)
  await open(nodes)
  await ready(nodes, 4)

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')

  const count = arr[0]?.followers?.length - 1
  t.ok(arr[0].nodeId !== 5, 'leader not node 5')

  arr = followers(nodes)
  t.equal(arr.length, 3, '3 followers')
  t.equal(count, 3, '3 followers again')

  const ok = arr.every((node) => node.nodeId !== 5)
  t.ok(ok, 'followers not node 5')

  let total = 0
  let error = false

  while (total < 15_000) {
    await sleep(100)
    total += 100

    arr = leaders(nodes)
    if (arr.length > 1) {
      t.equal(arr.length, 1, '1 leader')
      error = true
      break
    }

    const count = arr[0]?.followers?.length - 1
    arr = followers(nodes)
    if (arr.length === 4 && count === 4) {
      t.equal(arr.length, 4, '4 followers')
      break
    }
  }

  if (total >= 15_000) {
    t.fail('test timed out')
    error = true
  }

  arr = leaders(nodes)
  if (arr.length !== 1) {
    t.equal(arr.length, 1, '1 leader end')
    error = true
  }
  t.ok(!error, 'no error')
})

test('test elect n=5 and 2 offline', async (t) => {
  t.plan(7)
  t.teardown(() => close(nodes))

  const allowSend = (to, from, msg) => from !== 4 && from !== 5
  const coms = comms(allowSend)
  const nodes = connect(coms, 5)
  await open(nodes)
  await ready(nodes, 3)

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')
  t.ok(arr[0].nodeId !== 4, 'leader not node 4')
  t.ok(arr[0].nodeId !== 5, 'leader not node 5')

  const count = arr[0]?.followers?.length - 1
  arr = followers(nodes)
  t.equal(arr.length, 2, '2 followers')
  t.equal(count, 2, '2 followers again')

  let ok = arr.every((node) => node.nodeId !== 4)
  t.ok(ok, 'followers not node 4')

  ok = arr.every((node) => node.nodeId !== 5)
  t.ok(ok, 'followers not node 5')
})

test('test elect n=5 and 2 delayed', async (t) => {
  t.teardown(() => close(nodes))

  const delaySend = (to, from, msg) => (from === 4 || from === 5) ? 1_000 : 0
  const coms = comms(undefined, delaySend)
  const nodes = connect(coms, 5)
  await open(nodes)
  await ready(nodes, 3)

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')
  const count = arr[0]?.followers?.length - 1
  t.ok(arr[0].nodeId !== 4, 'leader not node 4')
  t.ok(arr[0].nodeId !== 5, 'leader not node 5')

  arr = followers(nodes)
  t.equal(arr.length, 2, '2 followers')
  t.equal(count, 2, '2 followers again')

  let ok = arr.every((node) => node.nodeId !== 4)
  t.ok(ok, 'followers not node 4')

  ok = arr.every((node) => node.nodeId !== 5)
  t.ok(ok, 'followers not node 5')

  let total = 0
  let error = false

  while (total < 15_000) {
    await sleep(100)
    total += 100

    arr = leaders(nodes)
    if (arr.length > 1) {
      t.equal(arr.length, 1, '1 leader')
      error = true
      break
    }

    const count = arr[0]?.followers?.length - 1
    arr = followers(nodes)
    if (arr.length === 4 && count === 4) {
      t.equal(arr.length, 4, '4 followers')
      break
    }
  }

  if (total >= 15_000) {
    t.fail('test timed out')
    error = true
  }

  arr = leaders(nodes)
  if (arr.length !== 1) {
    t.equal(arr.length, 1, '1 leader end')
    error = true
  }
  t.ok(!error, 'no error')
})
