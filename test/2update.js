const test = require('tape')
const lib = require('../index.js')
const { open, comms, sleep, start, stop, leaders, followers } = require('./util.js')

test('test n=5 update n=7', async (t) => {
  const coms = comms()
  let nodes = open(coms, 5)
  start(nodes)
  await sleep(100)

  const ok = nodes.every((node) => [1, 2, 3, 4, 5].includes(node.nodeId))
  t.ok(ok, 'ids correct')

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')
  arr = followers(nodes)
  t.equal(arr.length, 4, '4 followers')

  let more = open(coms, 7, 6)
  t.equal(more.length, 2, 'open 2 more')
  t.equal(more[0].nodeId, 6, 'id = 6')
  t.equal(more[1].nodeId, 7, 'id = 7')
  start(more)

  nodes = [...nodes, ...more]
  const ids = nodes.map((node) => node.nodeId)
  nodes.forEach((node) => node.setNodes(ids))

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

    arr = followers(nodes)
    if (arr.length === 6) {
      t.equal(arr.length, 6, '6 followers')
      break
    }
  }

  arr = leaders(nodes)
  if (arr.length !== 1) {
    t.equal(arr.length, 1, '1 leader')
    error = true
  }

  t.teardown(() => coms.close())
})

test('test n=7 update n=5', async (t) => {
  const coms = comms()
  let nodes = open(coms, 7)
  start(nodes)
  await sleep(100)

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')
  arr = followers(nodes)
  t.equal(arr.length, 6, '6 followers')

  nodes = nodes.filter((node) => node.nodeId !== 6)
  nodes = nodes.filter((node) => node.nodeId !== 7)
  // todo: old nodes think they are leader

  const ids = nodes.map((node) => node.nodeId)
  nodes.forEach((node) => node.setNodes(ids))

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

    const count = arr[0]?.followers?.length

    if (count === 4) {
      t.equal(count, 4, '4 followers')
      break
    }
  }

  arr = leaders(nodes)
  if (arr.length !== 1) {
    t.equal(arr.length, 1, '1 leader')
    error = true
  }

  t.ok(!error, 'no error')
  t.teardown(() => coms.close())
})
