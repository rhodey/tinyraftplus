const test = require('tape')
const lib = require('../index.js')
const { open, comms, sleep, start, stop, leaders, followers } = require('./util.js')

test('test log n=5', async (t) => {
  t.plan(4)
  const coms = comms()
  const nodes = open(coms, 5)
  start(nodes)
  await sleep(100)

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')
  const count = arr[0]?.followers?.length - 1

  arr = followers(nodes)
  t.equal(arr.length, 4, '4 followers')
  t.equal(count, 4, '4 followers again')

  const node = arr[0]
  const data = { op: 'hello', args: 'world' }

  await node.append(data)
  t.ok(true, 'append ok')

  t.teardown(() => stop(nodes))
})


