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
