const test = require('tape')
const { FsLog } = require('../index.js')
const { connect, comms, open, close, ready } = require('./util.js')

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
  t.deepEqual(toObj(node.log.head), data, `${name} head = data`)
}

const testHeadMulti = (t, data, nodes) => nodes.forEach((node) => testHead(t, data, node))

test('test elect n=3 then append 6', async (t) => {
  t.teardown(() => close(nodes))
  const coms = comms()
  const logs = (id) => new FsLog('/tmp/', `node-${id}`)
  const opts = { minFollowers: 2 } // force full repl
  const nodes = connect(coms, 3, null, opts, logs)
  await reset(nodes)
  await open(nodes)
  await ready(nodes)

  const ok = nodes.every((node) => [1, 2, 3].includes(node.nodeId))
  t.ok(ok, 'ids correct')

  const leader = leaders(nodes)[0]
  const flw = followers(nodes)
  t.equal(flw.length, 2, '2 followers')

  // leader
  let data = { a: 1 }
  let seq = await leader.append(toBuf(data))
  testSeqMulti(t, seq, 0n, 0n, nodes)
  testHeadMulti(t, data, nodes)

  data = { bb: 2 }
  seq = await leader.append(toBuf(data))
  testSeqMulti(t, seq, 1n, 1n, nodes)
  testHeadMulti(t, data, nodes)

  // follower 1
  data = { ccc: 3 }
  seq = await flw[0].append(toBuf(data))
  testSeqMulti(t, seq, 2n, 2n, nodes)
  testHeadMulti(t, data, nodes)

  data = { dd: 4 }
  seq = await flw[0].append(toBuf(data))
  testSeqMulti(t, seq, 3n, 3n, nodes)
  testHeadMulti(t, data, nodes)

  // restart
  await close(nodes)
  await open(nodes)
  await ready(nodes)

  // follower 2
  data = { e: 5 }
  seq = await flw[1].append(toBuf(data))
  testSeqMulti(t, seq, 4n, 4n, nodes)
  testHeadMulti(t, data, nodes)

  data = { a: 6 }
  seq = await flw[1].append(toBuf(data))
  testSeqMulti(t, seq, 5n, 5n, nodes)
  testHeadMulti(t, data, nodes)
})

test('test elect n=3 then append batch', async (t) => {
  t.teardown(() => close(nodes))
  const coms = comms()
  const logs = (id) => new FsLog('/tmp/', `node-${id}`)
  const opts = { minFollowers: 2 } // force full repl
  const nodes = connect(coms, 3, null, opts, logs)
  await reset(nodes)
  await open(nodes)
  await ready(nodes)

  const leader = leaders(nodes)[0]
  const flw = followers(nodes)

  let data = { a: 1 }
  let seq = await leader.append(toBuf(data))
  testSeqMulti(t, seq, 0n, 0n, nodes)
  testHeadMulti(t, data, nodes)

  // leader batch
  data = [{ b: 2 }, { c: 3 }]
  seq = await leader.appendBatch(data.map(toBuf))
  testSeqMulti(t, seq, 1n, 2n, nodes)
  testHeadMulti(t, data[1], nodes)

  data = { d: 4 }
  seq = await leader.append(toBuf(data))
  testSeqMulti(t, seq, 3n, 3n, nodes)
  testHeadMulti(t, data, nodes)

  // follower 1 batch
  data = [{ a: 1 }, { b: 2 }]
  seq = await flw[0].appendBatch(data.map(toBuf))
  testSeqMulti(t, seq, 4n, 5n, nodes)
  testHeadMulti(t, data[1], nodes)

  data = { c: 3 }
  seq = await flw[0].append(toBuf(data))
  testSeqMulti(t, seq, 6n, 6n, nodes)
  testHeadMulti(t, data, nodes)

  // follower 2 batch
  data = [{ a: 1 }, { b: 2 }]
  seq = await flw[1].appendBatch(data.map(toBuf))
  testSeqMulti(t, seq, 7n, 8n, nodes)
  testHeadMulti(t, data[1], nodes)

  // restart
  await close(nodes)
  await open(nodes)
  await ready(nodes)

  data = { c: 3 }
  seq = await flw[1].append(toBuf(data))
  testSeqMulti(t, seq, 9n, 9n, nodes)
  testHeadMulti(t, data, nodes)
})
