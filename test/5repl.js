const test = require('tape')
const { TinyRaftPlus, TinyRaftLog } = require('../index.js')
const { sleep, open, comms, start, stop } = require('./util.js')

const leaders = (nodes) => nodes.filter((node) => node.state === 'leader')

const followers = (nodes) => nodes.filter((node) => node.state === 'follower')

const reset = (nodes) => Promise.all(nodes.map((node) => node.log.del()))

const toBuf = (obj) => {
  if (obj === null) { return null }
  obj = JSON.stringify(obj)
  return Buffer.from(obj, 'utf8')
}

const toObj = (buf) => {
  if (buf === null) { return null }
  return JSON.parse(buf.toString('utf8'))
}

const testSeq = (t, a, b, c, node) => {
  const name = `node ${node.nodeId} ${node.state}`
  t.equal(a, b, `${name} seq = ${b}`)
  t.equal(node.log.seq, c, `${name} log seq = ${c}`)
}

const testSeqMulti = (t, a, b, c, nodes) => nodes.forEach((node) => testSeq(t, a, b, c, node))

const testHead = (t, data, node) => {
  const name = `node ${node.nodeId} ${node.state}`
  t.deepEqual(node.log.head, data, `${name} head = data`)
}

const testHeadMulti = (t, data, nodes) => nodes.forEach((node) => testHead(t, data, node))

test('test elect n=3 then append 6', async (t) => {
  t.teardown(() => stop(nodes))
  const coms = comms()
  const logs = (id) => new FsLog('/tmp/', `node-${id}`)
  const opts = { minFollowers: 2 } // force full repl
  const nodes = open(coms, 3, null, {}, logs)
  await reset(nodes)
  await start(nodes)
  await sleep(100)

  let ok = nodes.every((node) => [1, 2, 3].includes(node.nodeId))
  t.ok(ok, 'ids correct')

  let leader = leaders(nodes)
  t.equal(leader.length, 1, '1 leader')
  leader = leader[0]

  const flw = followers(nodes)
  t.equal(flw.length, 2, '2 followers')

  // leader
  let data = { a: 1 }
  ok = await leader.append(data)
  testSeqMulti(t, ok.seq, '0', '0', nodes)
  testHeadMulti(t, data, nodes)

  data = { a: 2 }
  ok = await leader.append(data)
  testSeqMulti(t, ok.seq, '1', '1', nodes)
  testHeadMulti(t, data, nodes)

  // follower 1
  data = { a: 3 }
  ok = await flw[0].append(data)
  testSeqMulti(t, ok.seq, '2', '2', nodes)
  testHeadMulti(t, data, nodes)

  data = { a: 4 }
  ok = await flw[0].append(data)
  testSeqMulti(t, ok.seq, '3', '3', nodes)
  testHeadMulti(t, data, nodes)

  // follower 2
  data = { a: 5 }
  ok = await flw[1].append(data)
  testSeqMulti(t, ok.seq, '4', '4', nodes)
  testHeadMulti(t, data, nodes)

  data = { a: 6 }
  ok = await flw[1].append(data)
  testSeqMulti(t, ok.seq, '5', '5', nodes)
  testHeadMulti(t, data, nodes)
})

test('test elect n=3 then append batch', async (t) => {
  t.teardown(() => stop(nodes))
  const coms = comms()
  const logs = (id) => new FsLog('/tmp/', `node-${id}`)
  const opts = { minFollowers: 2 } // force full repl
  const nodes = open(coms, 3, null, {}, logs)
  await reset(nodes)
  await start(nodes)
  await sleep(100)

  const leader = leaders(nodes)[0]
  const flw = followers(nodes)

  let data = { a: 1 }
  let ok = await leader.append(data)
  testSeqMulti(t, ok.seq, '0', '0', nodes)
  testHeadMulti(t, data, nodes)

  // leader batch
  data = [{ b: 2 }, { c: 3 }]
  ok = await leader.appendBatch(data)
  testSeqMulti(t, ok.seq, '1', '2', nodes)
  testHeadMulti(t, data[1], nodes)

  data = { d: 4 }
  ok = await leader.append(data)
  testSeqMulti(t, ok.seq, '3', '3', nodes)
  testHeadMulti(t, data, nodes)

  // follower 1 batch
  data = [{ a: 1 }, { b: 2 }]
  ok = await flw[0].appendBatch(data)
  testSeqMulti(t, ok.seq, '4', '5', nodes)
  testHeadMulti(t, data[1], nodes)

  data = { c: 3 }
  ok = await flw[0].append(data)
  testSeqMulti(t, ok.seq, '6', '6', nodes)
  testHeadMulti(t, data, nodes)

  // follower 2 batch
  data = [{ a: 1 }, { b: 2 }]
  ok = await flw[1].appendBatch(data)
  testSeqMulti(t, ok.seq, '7', '8', nodes)
  testHeadMulti(t, data[1], nodes)

  data = { c: 3 }
  ok = await flw[1].append(data)
  testSeqMulti(t, ok.seq, '9', '9', nodes)
  testHeadMulti(t, data, nodes)
})
