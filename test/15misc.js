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

test('test elect n=3 then del 1 follower', async (t) => {
  t.teardown(() => close(nodes))
  const coms = comms()
  const nodes = connect(coms, 3, null)
  await reset(nodes)
  await open(nodes)
  await ready(nodes)

  let leader = leaders(nodes)[0]
  let flw = followers(nodes)
  t.equal(flw.length, 2, '2 followers')

  await leader.awaitEvent('commit', () => true)
  let s = leader.seq

  let data = { a: 1 }
  let seq = await leader.append(toBuf(data))
  t.equal(seq, s + 1n, `leader ret = ${s + 1n}`)
  t.equal(leader.seq, s + 1n, `leader log seq = ${s + 1n}`)

  data = { bb: 2 }
  seq = await leader.append(toBuf(data))
  t.equal(seq, s + 2n, `leader ret = ${s + 1n}`)
  t.equal(leader.seq, s + 2n, `leader log seq = ${s + 1n}`)

  // del
  await close([flw[0]])
  await flw[0].log.del()
  await open([flw[0]])
  await ready(nodes)

  leader = leaders(nodes)[0]
  flw = followers(nodes)
  s = seq

  data = { ccc: 3 }
  seq = await leader.append(toBuf(data))

  const next = s + 1n
  const name = `node ${leader.id} ${leader.state}`
  t.equal(seq, next, `${name} ret = ${next}`)
  t.equal(leader.seq, next, `${name} seq = ${next}`)
  t.equal(leader.log.seq, next, `${name} log seq = ${next}`)

  await sleep(500)
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  testHeadMulti(t, data, nodes)

  // full
  data = [{ a: 1 }, { bb: 2 }, { ccc: 3 }]
  for (const node of nodes) {
    let count = 0n
    for await (let next of node.log.iter(count)) {
      next = next.subarray(8)
      if (next.length <= 0) { continue }
      next = toObj(next)
      t.deepEqual(next, data[count], `node ${node.id} data ${count} ok`)
      count++
    }
  }
})

test('test elect n=3 then del 1 leader', async (t) => {
  t.teardown(() => close(nodes))
  const coms = comms()
  const nodes = connect(coms, 3, null)
  await reset(nodes)
  await open(nodes)
  await ready(nodes)

  let leader = leaders(nodes)[0]
  let flw = followers(nodes)
  t.equal(flw.length, 2, '2 followers')

  await leader.awaitEvent('commit', () => true)
  let s = leader.seq

  let data = { a: 1 }
  let seq = await leader.append(toBuf(data))
  t.equal(seq, s + 1n, `leader ret = ${s + 1n}`)
  t.equal(leader.seq, s + 1n, `leader log seq = ${s + 1n}`)

  data = { bb: 2 }
  seq = await leader.append(toBuf(data))
  t.equal(seq, s + 2n, `leader ret = ${s + 1n}`)
  t.equal(leader.seq, s + 2n, `leader log seq = ${s + 1n}`)

  // del
  console.log('leader =>', leader.id, seq)
  const copy = leader
  await close([copy])
  await copy.log.del()

  // make elect
  await sleep(5000)
  await ready(flw)
  leader = leaders(flw)[0]
  s = leader.seq
  console.log('leader =>', leader.id, s)

  // restart
  await open([copy])
  await ready(nodes)
  leader = leaders(nodes)[0]
  flw = followers(nodes)
  s = leader.seq
  console.log('leader =>', leader.id, s)

  data = { ccc: 3 }
  seq = await leader.append(toBuf(data))

  const next = s + 1n
  const name = `node ${leader.id} ${leader.state}`
  t.equal(seq, next, `${name} ret = ${next}`)
  t.equal(leader.seq, next, `${name} seq = ${next}`)
  t.equal(leader.log.seq, next, `${name} log seq = ${next}`)

  await sleep(1000)
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  testHeadMulti(t, data, nodes)

  // full
  data = [{ a: 1 }, { bb: 2 }, { ccc: 3 }]
  for (const node of nodes) {
    let count = 0n
    for await (let next of node.log.iter(count)) {
      next = next.subarray(8)
      if (next.length <= 0) { continue }
      next = toObj(next)
      t.deepEqual(next, data[count], `node ${node.id} data ${count} ok`)
      count++
    }
  }
})
