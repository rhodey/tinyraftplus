const test = require('tape')
const { TinyRaftPlus, TinyRaftLog } = require('../index.js')
const { open, comms, sleep, start, stop, leaders, followers } = require('./util.js')

test('test elect n=3 then append 5', async (t) => {
  t.plan(10)
  const coms = comms()
  const logs = () => new TinyRaftLog()
  const nodes = open(coms, 3, null, {}, logs)
  await start(nodes)
  await sleep(100)

  const ok = nodes.every((node) => [1, 2, 3].includes(node.nodeId))
  t.ok(ok, 'ids correct')

  let leader = leaders(nodes)
  t.equal(leader.length, 1, '1 leader')
  leader = leader[0]

  const arr = followers(nodes)
  const count = leader.followers?.length - 1
  t.equal(arr.length, 2, '2 followers')
  t.equal(count, 2, '2 followers again')

  let seq = await leader.append({ a: 1 })
  t.equal(seq, '0', 'seq = 0')
  seq = await leader.append({ a: 2 })
  t.equal(seq, '1', 'seq = 1')

  seq = await arr[0].append({ a: 3 })
  t.equal(seq, '2', 'seq = 2')
  seq = await arr[0].append({ a: 4 })
  t.equal(seq, '3', 'seq = 3')

  seq = await arr[0].append({ a: 5 })
  t.equal(seq, '4', 'seq = 4')
  seq = await arr[0].append({ a: 6 })
  t.equal(seq, '5', 'seq = 5')

  t.teardown(() => stop(nodes))
})
