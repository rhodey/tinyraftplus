const test = require('tape')
const { TinyRaftPlus, TinyRaftLog } = require('../index.js')
const { open, comms, sleep, start, stop, leaders, followers } = require('./util.js')

// todo: support and test repl restore

test('test elect n=3 then append 6', async (t) => {
  t.plan(16)
  const coms = comms()
  const logs = () => new TinyRaftLog()
  const opts = { minFollowers: 2 }
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

  // leader
  let data = { a: 1 }
  let seq = await leader.append(data)
  t.equal(seq, '0', 'seq = 0')
  t.deepEqual(leader.log.head, data, 'data = head')

  data = { a: 2 }
  seq = await leader.append(data)
  t.equal(seq, '1', 'seq = 1')
  t.deepEqual(leader.log.head, data, 'data = head')

  // follower 1
  data = { a: 3 }
  seq = await arr[0].append(data)
  t.equal(seq, '2', 'seq = 2')
  t.deepEqual(arr[0].log.head, data, 'data = head')

  data = { a: 4 }
  seq = await arr[0].append(data)
  t.equal(seq, '3', 'seq = 3')
  t.deepEqual(arr[0].log.head, data, 'data = head')

  // follower 2
  data = { a: 5 }
  seq = await arr[1].append(data)
  t.equal(seq, '4', 'seq = 4')
  t.deepEqual(arr[1].log.head, data, 'data = head')

  data = { a: 6 }
  seq = await arr[1].append(data)
  t.equal(seq, '5', 'seq = 5')
  t.deepEqual(arr[1].log.head, data, 'data = head')

  t.teardown(() => stop(nodes))
})
