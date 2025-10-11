const test = require('tape')
const { sleep, comms, connect } = require('./util.js')
const { open, close, ready } = require('./util.js')
const { leaders, followers } = require('./util.js')

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
  const name = `node ${node.id} ${node.state}`
  t.equal(a, b, `${name} ret = ${b}`)
  t.equal(node.seq, c, `${name} seq = ${c}`)
  t.equal(node.log.seq, c, `${name} log seq = ${c}`)
}

const testSeqMulti = (t, a, b, c, nodes) => nodes.forEach((node) => testSeq(t, a, b, c, node))

const testHead = (t, data, node) => {
  const name = `node ${node.id} ${node.state}`
  t.deepEqual(toObj(node.head), data, `${name} head = data`)
  const logHead = !node.log.head ? null : node.log.head.subarray(8)
  t.deepEqual(toObj(logHead), data, `${name} log head = data`)
}

const testHeadMulti = (t, data, nodes) => nodes.forEach((node) => testHead(t, data, node))

test('test elect n=3 then append 6', async (t) => {
  t.teardown(() => close(nodes))
  const coms = comms()

  let myCount = 0n
  const apply = (bufs) => {
    const results = []
    bufs.forEach((buf) => results.push(++myCount))
    return results
  }
  const read = (cmd) => {
    if (cmd !== 123) { throw new Error('bad cmd') }
    return myCount
  }
  const opts = () => ({ apply, read, quorum: 3 })

  const nodes = connect(coms, 3, null, opts)
  await reset(nodes)
  await open(nodes)
  await ready(nodes)

  let leader = leaders(nodes)[0]
  let flw = followers(nodes)
  flw.forEach((node) => node.opts.apply = null)
  t.equal(flw.length, 2, '2 followers')

  await leader.awaitEvent('commit', () => true)
  let s = leader.seq
  await leader.awaitEvent('apply', () => true)
  myCount = 0n

  // leader
  let data = { a: 1 }
  let ok = await leader.append(toBuf(data))
  let [seq, result] = ok
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 1n, `1 = result`)

  // read
  ok = await leader.read(123)
  seq = ok[0]; result = ok[1]
  t.equal(seq, s + 1n, `seq ok`)
  t.equal(result, 1n, `1 = result`)

  data = { bb: 2 }
  ok = await leader.append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 2n, s + 2n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 2n, `2 = result`)

  ok = await leader.read(123)
  seq = ok[0]; result = ok[1]
  t.equal(seq, s + 2n, `seq ok`)
  t.equal(result, 2n, `2 = result`)

  // follower 1
  data = { ccc: 3 }
  ok = await flw[0].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 3n, s + 3n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 3n, `3 = result`)

  ok = await flw[0].read(123)
  seq = ok[0]; result = ok[1]
  t.equal(seq, s + 3n, `seq ok`)
  t.equal(result, 3n, `3 = result`)

  data = { dd: 4 }
  ok = await flw[0].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 4n, s + 4n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 4n, `4 = result`)

  ok = await flw[0].read(123)
  seq = ok[0]; result = ok[1]
  t.equal(seq, s + 4n, `seq ok`)
  t.equal(result, 4n, `4 = result`)

  // restart
  await close(nodes)
  await open(nodes)
  await ready(nodes)

  leader = leaders(nodes)[0]
  leader.opts.apply = apply
  flw = followers(nodes)
  flw.forEach((node) => node.opts.apply = null)
  await leader.awaitEvent('commit', () => true)
  s = leader.seq
  await leader.awaitEvent('apply', () => true)
  myCount = 0n

  // follower 2
  data = { e: 5 }
  ok = await flw[1].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 1n, `1 = result`)

  ok = await flw[1].read(123)
  seq = ok[0]; result = ok[1]
  t.equal(seq, s + 1n, `seq ok`)
  t.equal(result, 1n, `1 = result`)

  data = { a: 6 }
  ok = await flw[1].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 2n, s + 2n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 2n, `2 = result`)

  ok = await flw[1].read(123)
  seq = ok[0]; result = ok[1]
  t.equal(seq, s + 2n, `seq ok`)
  t.equal(result, 2n, `2 = result`)
})

test('test elect n=3 then append 6 - async', async (t) => {
  t.teardown(() => close(nodes))
  const coms = comms()

  let myCount = 0n
  const apply = async (bufs) => {
    await sleep(50)
    const results = []
    bufs.forEach((buf) => results.push(++myCount))
    return results
  }
  const read = async (cmd) => {
    await sleep(50)
    if (cmd !== 123) { throw new Error('bad cmd') }
    return myCount
  }
  const opts = () => ({ apply, read, quorum: 3 })

  const nodes = connect(coms, 3, null, opts)
  await reset(nodes)
  await open(nodes)
  await ready(nodes)

  let leader = leaders(nodes)[0]
  let flw = followers(nodes)
  flw.forEach((node) => node.opts.apply = null)
  t.equal(flw.length, 2, '2 followers')

  await leader.awaitEvent('commit', () => true)
  let s = leader.seq
  await leader.awaitEvent('apply', () => true)
  myCount = 0n

  // leader
  let data = { a: 1 }
  let ok = await leader.append(toBuf(data))
  let [seq, result] = ok
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 1n, `1 = result`)

  // read
  ok = await leader.read(123)
  seq = ok[0]; result = ok[1]
  t.equal(seq, s + 1n, `seq ok`)
  t.equal(result, 1n, `1 = result`)

  data = { bb: 2 }
  ok = await leader.append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 2n, s + 2n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 2n, `2 = result`)

  ok = await leader.read(123)
  seq = ok[0]; result = ok[1]
  t.equal(seq, s + 2n, `seq ok`)
  t.equal(result, 2n, `2 = result`)

  // follower 1
  data = { ccc: 3 }
  ok = await flw[0].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 3n, s + 3n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 3n, `3 = result`)

  ok = await flw[0].read(123)
  seq = ok[0]; result = ok[1]
  t.equal(seq, s + 3n, `seq ok`)
  t.equal(result, 3n, `3 = result`)

  data = { dd: 4 }
  ok = await flw[0].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 4n, s + 4n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 4n, `4 = result`)

  ok = await flw[0].read(123)
  seq = ok[0]; result = ok[1]
  t.equal(seq, s + 4n, `seq ok`)
  t.equal(result, 4n, `4 = result`)

  // restart
  await close(nodes)
  await open(nodes)
  await ready(nodes)

  leader = leaders(nodes)[0]
  leader.opts.apply = apply
  flw = followers(nodes)
  flw.forEach((node) => node.opts.apply = null)
  await leader.awaitEvent('commit', () => true)
  s = leader.seq
  await leader.awaitEvent('apply', () => true)
  myCount = 0n

  // follower 2
  data = { e: 5 }
  ok = await flw[1].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 1n, `1 = result`)

  ok = await flw[1].read(123)
  seq = ok[0]; result = ok[1]
  t.equal(seq, s + 1n, `seq ok`)
  t.equal(result, 1n, `1 = result`)

  data = { a: 6 }
  ok = await flw[1].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 2n, s + 2n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 2n, `2 = result`)

  ok = await flw[1].read(123)
  seq = ok[0]; result = ok[1]
  t.equal(seq, s + 2n, `seq ok`)
  t.equal(result, 2n, `2 = result`)
})

test('test elect n=3 then append batch', async (t) => {
  t.teardown(() => close(nodes))
  const coms = comms()

  let myCount = 0n
  const apply = (bufs) => {
    const results = []
    bufs.forEach((buf) => results.push(++myCount))
    return results
  }
  const read = (cmd) => {
    if (cmd !== 123) { throw new Error('bad cmd') }
    return myCount
  }
  const opts = () => ({ apply, read, quorum: 3 })

  const nodes = connect(coms, 3, null, opts)
  await reset(nodes)
  await open(nodes)
  await ready(nodes)

  let leader = leaders(nodes)[0]
  let flw = followers(nodes)
  flw.forEach((node) => node.opts.apply = null)
  t.equal(flw.length, 2, '2 followers')

  await leader.awaitEvent('commit', () => true)
  let s = leader.seq
  await leader.awaitEvent('apply', () => true)
  myCount = 0n

  // leader
  let data = { a: 1 }
  let ok = await leader.append(toBuf(data))
  let [seq, result] = ok
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 1n, `1 = result`)

  // read
  ok = await leader.read(123)
  seq = ok[0]; result = ok[1]
  t.equal(seq, s + 1n, `seq ok`)
  t.equal(result, 1n, `1 = result`)

  // leader batch
  data = [{ b: 2 }, { c: 3 }]
  ok = await leader.appendBatch(data.map(toBuf))
  seq = ok[0]
  let results = ok[1]
  testSeqMulti(t, seq, s + 2n, s + 3n, nodes)
  testHeadMulti(t, data[1], nodes)
  t.equal(results.length, 2, `2 result`)
  t.equal(results[0], 2n, `2 = result`)
  t.equal(results[1], 3n, `3 = result`)

  ok = await leader.read(123)
  seq = ok[0]; result = ok[1]
  t.equal(seq, s + 3n, `seq ok`)
  t.equal(result, 3n, `3 = result`)

  // follower 1 batch
  data = [{ a: 1 }, { b: 2 }]
  ok = await flw[0].appendBatch(data.map(toBuf))
  seq = ok[0]; results = ok[1]
  testSeqMulti(t, seq, s + 4n, s + 5n, nodes)
  testHeadMulti(t, data[1], nodes)
  t.equal(results.length, 2, `2 result`)
  t.equal(results[0], 4n, `4 = result`)
  t.equal(results[1], 5n, `5 = result`)

  ok = await flw[0].read(123)
  seq = ok[0]; result = ok[1]
  t.equal(seq, s + 5n, `seq ok`)
  t.equal(result, 5n, `5 = result`)

  data = { c: 3 }
  ok = await flw[0].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 6n, s + 6n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 6n, `6 = result`)

  ok = await flw[0].read(123)
  seq = ok[0]; result = ok[1]
  t.equal(seq, s + 6n, `seq ok`)
  t.equal(result, 6n, `6 = result`)

  // restart
  await close(nodes)
  await open(nodes)
  await ready(nodes)

  leader = leaders(nodes)[0]
  leader.opts.apply = apply
  flw = followers(nodes)
  flw.forEach((node) => node.opts.apply = null)
  await leader.awaitEvent('commit', () => true)
  s = leader.seq
  await leader.awaitEvent('apply', () => true)
  myCount = 0n

  data = { c: 3 }
  ok = await flw[1].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 1n, `1 = result`)

  ok = await flw[1].read(123)
  seq = ok[0]; result = ok[1]
  t.equal(seq, s + 1n, `seq ok`)
  t.equal(result, 1n, `1 = result`)
})

test('test elect n=3 then append batch - async', async (t) => {
  t.teardown(() => close(nodes))
  const coms = comms()

  let myCount = 0n
  const apply = async (bufs) => {
    await sleep(50)
    const results = []
    bufs.forEach((buf) => results.push(++myCount))
    return results
  }
  const read = async (cmd) => {
    await sleep(50)
    if (cmd !== 123) { throw new Error('bad cmd') }
    return myCount
  }
  const opts = () => ({ apply, read, quorum: 3 })

  const nodes = connect(coms, 3, null, opts)
  await reset(nodes)
  await open(nodes)
  await ready(nodes)

  let leader = leaders(nodes)[0]
  let flw = followers(nodes)
  flw.forEach((node) => node.opts.apply = null)
  t.equal(flw.length, 2, '2 followers')

  await leader.awaitEvent('commit', () => true)
  let s = leader.seq
  await leader.awaitEvent('apply', () => true)
  myCount = 0n

  // leader
  let data = { a: 1 }
  let ok = await leader.append(toBuf(data))
  let [seq, result] = ok
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 1n, `1 = result`)

  // read
  ok = await leader.read(123)
  seq = ok[0]; result = ok[1]
  t.equal(seq, s + 1n, `seq ok`)
  t.equal(result, 1n, `1 = result`)

  // leader batch
  data = [{ b: 2 }, { c: 3 }]
  ok = await leader.appendBatch(data.map(toBuf))
  seq = ok[0]
  let results = ok[1]
  testSeqMulti(t, seq, s + 2n, s + 3n, nodes)
  testHeadMulti(t, data[1], nodes)
  t.equal(results.length, 2, `2 result`)
  t.equal(results[0], 2n, `2 = result`)
  t.equal(results[1], 3n, `3 = result`)

  ok = await leader.read(123)
  seq = ok[0]; result = ok[1]
  t.equal(seq, s + 3n, `seq ok`)
  t.equal(result, 3n, `3 = result`)

  // follower 1 batch
  data = [{ a: 1 }, { b: 2 }]
  ok = await flw[0].appendBatch(data.map(toBuf))
  seq = ok[0]; results = ok[1]
  testSeqMulti(t, seq, s + 4n, s + 5n, nodes)
  testHeadMulti(t, data[1], nodes)
  t.equal(results.length, 2, `2 result`)
  t.equal(results[0], 4n, `4 = result`)
  t.equal(results[1], 5n, `5 = result`)

  ok = await flw[0].read(123)
  seq = ok[0]; result = ok[1]
  t.equal(seq, s + 5n, `seq ok`)
  t.equal(result, 5n, `5 = result`)

  data = { c: 3 }
  ok = await flw[0].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 6n, s + 6n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 6n, `6 = result`)

  ok = await flw[0].read(123)
  seq = ok[0]; result = ok[1]
  t.equal(seq, s + 6n, `seq ok`)
  t.equal(result, 6n, `6 = result`)

  // restart
  await close(nodes)
  await open(nodes)
  await ready(nodes)

  leader = leaders(nodes)[0]
  leader.opts.apply = apply
  flw = followers(nodes)
  flw.forEach((node) => node.opts.apply = null)
  await leader.awaitEvent('commit', () => true)
  s = leader.seq
  await leader.awaitEvent('apply', () => true)
  myCount = 0n

  data = { c: 3 }
  ok = await flw[1].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 1n, `1 = result`)

  ok = await flw[1].read(123)
  seq = ok[0]; result = ok[1]
  t.equal(seq, s + 1n, `seq ok`)
  t.equal(result, 1n, `1 = result`)
})
