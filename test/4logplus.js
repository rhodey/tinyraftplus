const test = require('tape')
const { TinyRaftPlus, TinyRaftLog } = require('../index.js')
const { open, comms, sleep, start, stop, leaders, followers } = require('./util.js')

// todo: support and test repl restore

const testSeq = (t, a, b, node) => {
  const name = `node ${node.nodeId} ${node.state}`
  t.equal(a, b, `${name} seq = ${b}`)
  t.equal(node.log.seq, b, `${name} seq = ${b}`)
}

const testSeqMulti = (t, a, b, nodes) => nodes.forEach((node) => testSeq(t, a, b, node))

const testHead = (t, data, node) => {
  const name = `node ${node.nodeId} ${node.state}`
  t.deepEqual(node.log.head, data, `${name} head = data`)
}

const testHeadMulti = (t, data, nodes) => nodes.forEach((node) => testHead(t, data, node))

test('test elect 3 then append 6', async (t) => {
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

  const flw = followers(nodes)
  const count = leader.followers?.length - 1
  t.equal(flw.length, 2, '2 followers')
  t.equal(count, 2, '2 followers again')

  // leader
  let data = { a: 1 }
  let seq = await leader.append(data)
  testSeqMulti(t, seq, '0', nodes)
  testHeadMulti(t, data, nodes)

  data = { a: 2 }
  seq = await leader.append(data)
  testSeqMulti(t, seq, '1', nodes)
  testHeadMulti(t, data, nodes)

  // follower 1
  data = { a: 3 }
  seq = await flw[0].append(data)
  testSeqMulti(t, seq, '2', nodes)
  testHeadMulti(t, data, nodes)

  data = { a: 4 }
  seq = await flw[0].append(data)
  testSeqMulti(t, seq, '3', nodes)
  testHeadMulti(t, data, nodes)

  // follower 2
  data = { a: 5 }
  seq = await flw[1].append(data)
  testSeqMulti(t, seq, '4', nodes)
  testHeadMulti(t, data, nodes)

  data = { a: 6 }
  seq = await flw[1].append(data)
  testSeqMulti(t, seq, '5', nodes)
  testHeadMulti(t, data, nodes)

  t.teardown(() => stop(nodes))
})

test('test elect 3 then append batch', async (t) => {
  t.plan(11)
  const coms = comms()
  const logs = () => new TinyRaftLog()
  const opts = { minFollowers: 2 }
  const nodes = open(coms, 3, null, {}, logs)
  await start(nodes)
  await sleep(100)

  const leader = leaders(nodes)[0]
  const flw = followers(nodes)

  let data = { a: 1 }
  let seq = await leader.append(data)
  t.equal(seq, '0', 'seq = 0')
  t.deepEqual(leader.log.head, data, 'data = head')

  data = [{ b: 2 }, { c: 3 }]
  seq = await leader.appendBatch(data)
  t.equal(seq, '1', 'seq = 1')
  t.equal(leader.log.seq, '2', 'seq = 2')
  t.deepEqual(leader.log.head, data[1], 'data = head')

  t.equal(flw[0].log.seq, '2', 'seq = 2')
  t.deepEqual(flw[0].log.head, data[1], 'data = head')

  t.equal(flw[1].log.seq, '2', 'seq = 2')
  t.deepEqual(flw[1].log.head, data[1], 'data = head')

  data = { d: 4 }
  seq = await leader.append(data)
  t.equal(seq, '3', 'seq = 4')
  t.deepEqual(leader.log.head, data, 'data = head')

  t.teardown(() => stop(nodes))
})
