const test = require('tape')
const { sleep, comms, connect } = require('./util.js')
const { open, close, ready } = require('./util.js')
const { leaders, followers } = require('./util.js')

const reset = (nodes) => Promise.all(nodes.map((node) => node.log.del()))

// todo: saved for later
return

test('test n=5 update n=7', async (t) => {
  t.teardown(() => close(nodes))

  const coms = comms()
  let nodes = connect(coms, 5)
  await reset(nodes)
  await open(nodes)
  await ready(nodes)

  const ok = nodes.every((node) => [1, 2, 3, 4, 5].includes(node.id))
  t.ok(ok, 'ids correct')

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')

  let count = arr[0].followers.length
  arr = followers(nodes)
  t.equal(arr.length, 4, '4 followers')
  t.equal(count, 4, '4 followers again')

  const more = connect(coms, 7, 6)
  await reset(more)
  t.equal(more.length, 2, 'connect 2 more')
  t.equal(more[0].id, 6, 'id = 6')
  t.equal(more[1].id, 7, 'id = 7')

  nodes = [...nodes, ...more]
  const ids = nodes.map((node) => node.id)
  nodes.forEach((node) => node.setNodes(ids))
  await open(more)

  let total = 0
  const timeout = 20_000
  while (total < timeout) {
    await sleep(100)
    total += 100

    arr = leaders(nodes)
    if (arr.length > 1) {
      t.equal(arr.length, 1, '1 leader')
      break
    } else if (arr.length <= 0) {
      continue
    }

    count = arr[0].followers.length
    if (count >= 6) { break }
  }

  t.equal(arr.length, 1, '1 leader')
  count = arr[0].followers.length
  t.equal(count, 6, '6 followers')
  total >= timeout && t.fail('timeout')
  t.end()
})

test('test n=7 update n=5', async (t) => {
  t.teardown(() => close(nodes))

  const coms = comms()
  let nodes = connect(coms, 7)
  await reset(nodes)
  await open(nodes)
  await ready(nodes)

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')

  let count = arr[0].followers.length
  arr = followers(nodes)
  t.equal(arr.length, 6, '6 followers')
  t.equal(count, 6, '6 followers again')

  await close([nodes[5], nodes[6]])
  nodes = nodes.filter((node) => node.id != 6)
  nodes = nodes.filter((node) => node.id != 7)

  const ids = nodes.map((node) => node.id)
  nodes.forEach((node) => node.setNodes(ids))

  let total = 0
  const timeout = 20_000
  while (total < timeout) {
    await sleep(100)
    total += 100

    arr = leaders(nodes)
    if (arr.length > 1) {
      t.equal(arr.length, 1, '1 leader')
      break
    } else if (arr.length <= 0) {
      continue
    }

    count = arr[0].followers.length
    if (count <= 4) { break }
  }

  t.equal(arr.length, 1, '1 leader')
  count = arr[0].followers.length
  t.equal(count, 4, '4 followers')
  total >= timeout && t.fail('timeout')
  t.end()
})
