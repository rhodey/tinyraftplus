const test = require('tape')
const { sleep, connect, comms, open, close, ready } = require('./util.js')

const leaders = (nodes) => nodes.filter((node) => node.state === 'leader')

const followers = (nodes) => nodes.filter((node) => node.state === 'follower')

test('test n=5 update n=7', async (t) => {
  t.teardown(() => close(nodes))

  const coms = comms()
  let nodes = connect(coms, 5)
  await open(nodes)
  await ready(nodes)

  const ok = nodes.every((node) => [1, 2, 3, 4, 5].includes(node.nodeId))
  t.ok(ok, 'ids correct')

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')

  const count = arr[0]?.followers?.length - 1
  arr = followers(nodes)
  t.equal(arr.length, 4, '4 followers')
  t.equal(count, 4, '4 followers again')

  const more = connect(coms, 7, 6)
  t.equal(more.length, 2, 'connect 2 more')
  t.equal(more[0].nodeId, 6, 'id = 6')
  t.equal(more[1].nodeId, 7, 'id = 7')
  await open(more)

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

    const count = arr[0]?.followers?.length - 1
    arr = followers(nodes)
    if (arr.length === 6 && count === 6) {
      t.equal(arr.length, 6, '6 followers')
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

/*
// todo: leader thinks has 6 followers
test('test n=7 update n=5', async (t) => {
  t.teardown(() => close(nodes))

  const coms = comms()
  let nodes = connect(coms, 7)
  await open(nodes)
  await ready(nodes)

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')
  arr = followers(nodes)
  t.equal(arr.length, 6, '6 followers')

  await close([nodes[5], nodes[6]])
  // todo: old nodes think they are leader
  nodes = nodes.filter((node) => node.nodeId !== 6)
  nodes = nodes.filter((node) => node.nodeId !== 7)

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

    const count = arr[0]?.followers?.length - 1
    arr = followers(nodes)
    // todo: leader thinks has 6 followers
    // console.log(123, arr.length, count)
    if (arr.length === 4 && count === 4) {
      t.equal(count, 4, '4 followers')
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
*/
