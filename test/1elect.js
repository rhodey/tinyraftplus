const test = require('tape')
const { open, comms, sleep, start, stop, leaders, followers } = require('./util.js')

// const { nodeId, nodes, state, leader, followers, term } = node

test('test elect n=3', async (t) => {
  t.plan(4)
  const coms = comms()
  const nodes = open(coms, 3)
  await start(nodes)
  await sleep(100)

  const ok = nodes.every((node) => [1, 2, 3].includes(node.nodeId))
  t.ok(ok, 'ids correct')

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')
  const count = arr[0]?.followers?.length - 1

  arr = followers(nodes)
  t.equal(arr.length, 2, '2 followers')
  t.equal(count, 2, '2 followers again')

  t.teardown(() => stop(nodes))
})

test('test elect n=3 and 1 offline', async (t) => {
  t.plan(5)
  const allowSend = (to, from, msg) => from !== 3
  const coms = comms(allowSend)
  const nodes = open(coms, 3)
  await start(nodes)
  await sleep(100)

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')
  t.ok(arr[0].nodeId !== 3, 'leader not node 3')
  const count = arr[0]?.followers?.length - 1

  arr = followers(nodes)
  t.equal(arr.length, 1, '1 follower')
  t.equal(count, 1, '1 follower again')
  t.ok(arr[0].nodeId !== 3, 'follower not node 3')

  t.teardown(() => stop(nodes))
})

test('test elect n=5', async (t) => {
  t.plan(3)
  const coms = comms()
  const nodes = open(coms, 5)
  await start(nodes)
  await sleep(100)

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')
  const count = arr[0]?.followers?.length - 1

  arr = followers(nodes)
  t.equal(arr.length, 4, '4 followers')
  t.equal(count, 4, '4 followers again')

  t.teardown(() => stop(nodes))
})

test('test elect n=5 and 1 offline', async (t) => {
  t.plan(5)
  const allowSend = (to, from, msg) => from !== 5
  const coms = comms(allowSend)
  const nodes = open(coms, 5)
  await start(nodes)
  await sleep(100)

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')
  t.ok(arr[0].nodeId !== 5, 'leader not node 5')
  const count = arr[0]?.followers?.length - 1

  arr = followers(nodes)
  t.equal(arr.length, 3, '3 followers')
  t.equal(count, 3, '3 followers again')
  const ok = arr.every((node) => node.nodeId !== 5)
  t.ok(ok, 'followers not node 5')

  t.teardown(() => stop(nodes))
})

test('test elect n=5 and 1 delayed', async (t) => {
  const delaySend = (to, from, msg) => from === 5 ? 1_000 : 0
  const coms = comms(undefined, delaySend)
  const nodes = open(coms, 5)
  await start(nodes)

  await sleep(100)
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
  t.teardown(() => stop(nodes))
})

test('test elect n=5 and 2 offline', async (t) => {
  t.plan(7)
  const allowSend = (to, from, msg) => from !== 4 && from !== 5
  const coms = comms(allowSend)
  const nodes = open(coms, 5)
  await start(nodes)
  await sleep(100)

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

  t.teardown(() => stop(nodes))
})

test('test elect n=5 and 2 delayed', async (t) => {
  const delaySend = (to, from, msg) => (from === 4 || from === 5) ? 1_000 : 0
  const coms = comms(undefined, delaySend)
  const nodes = open(coms, 5)
  await start(nodes)

  await sleep(100)
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
  t.teardown(() => stop(nodes))
})
