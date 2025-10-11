const test = require('tape')
const { FsLog } = require('../src/index.js')
const { Encoder, XxHashEncoder } = require('../src/index.js')

const { comms, connect } = require('./util.js')
const { open, close, ready } = require('./util.js')
const { leaders, followers } = require('./util.js')

const reset = (nodes) => Promise.all(nodes.map((node) => node.log.del()))

const toBuf = (obj) => {
  if (obj === null) { return null }
  obj = JSON.stringify(obj)
  return Buffer.from(obj, 'utf8')
}

async function testAppendSmall(t, encoder) {
  t.plan(1)
  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.open()

  const data = []
  const count = 100
  for (let i = 0; i < count; i++) {
    data.push(toBuf({ i }))
  }

  const begin = Date.now()
  for (const buf of data) {
    await log.append(buf)
  }

  const ms = Date.now() - begin
  console.log(`done ${count} in ${ms}ms`)

  const seconds = ms / 1000
  let avg = (count / seconds).toFixed(1)
  console.log(`${avg} append per second`)

  avg = (avg / 1000).toFixed(2)
  console.log(`${avg} append per ms`)

  t.pass('ok')
  t.teardown(() => log.close())
  console.log(`\n`)
}

test('test append 100 small', (t) => testAppendSmall(t, new Encoder()))
test('test append 100 small - xxhash body', (t) => testAppendSmall(t, new XxHashEncoder()))
test('test append 100 small - xxhash no body', (t) => testAppendSmall(t, new XxHashEncoder(false)))

async function testAppendLarge(t, encoder) {
  t.plan(1)
  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.open()

  const data = []
  const count = 100
  const large = new Array(1024).fill('a').join('')
  for (let i = 0; i < count; i++) {
    data.push(toBuf({ i, large }))
  }

  const begin = Date.now()
  for (const buf of data) {
    await log.append(buf)
  }

  const ms = Date.now() - begin
  console.log(`done ${count} in ${ms}ms`)

  const seconds = ms / 1000
  let avg = (count / seconds).toFixed(1)
  console.log(`${avg} append per second`)

  avg = (avg / 1000).toFixed(2)
  console.log(`${avg} append per ms`)

  t.pass('ok')
  t.teardown(() => log.close())
  console.log(`\n`)
}

test('test append 100 large', (t) => testAppendLarge(t, new Encoder()))
test('test append 100 large - xxhash body', (t) => testAppendLarge(t, new XxHashEncoder()))
test('test append 100 large - xxhash no body', (t) => testAppendLarge(t, new XxHashEncoder(false)))

async function testAppendLargeTxn(t, encoder) {
  t.plan(1)
  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.open()

  const data = []
  const count = 1_000
  const large = new Array(1024).fill('a').join('')
  for (let i = 0; i < count; i++) {
    data.push(toBuf({ i, large }))
  }

  const begin = Date.now()
  const txn = await log.txn()
  for (const buf of data) {
    await txn.append(buf)
  }

  await txn.commit()
  const ms = Date.now() - begin
  console.log(`done ${count} in ${ms}ms`)

  const seconds = ms / 1000
  let avg = (count / seconds).toFixed(1)
  console.log(`${avg} append per second`)

  avg = (avg / 1000).toFixed(2)
  console.log(`${avg} append per ms`)

  t.pass('ok')
  t.teardown(() => log.close())
  console.log(`\n`)
}

test('test append 1000 large txn', (t) => testAppendLargeTxn(t, new Encoder()))
test('test append 1000 large txn - xxhash body', (t) => testAppendLargeTxn(t, new XxHashEncoder()))
test('test append 1000 large txn - xxhash no body', (t) => testAppendLargeTxn(t, new XxHashEncoder(false)))

async function testAppendLargeNodes(t, encoder) {
  t.plan(2)
  t.teardown(() => close(nodes))
  const coms = comms()
  const nodes = connect(coms, 3)

  await reset(nodes)
  await open(nodes)
  await ready(nodes)

  const leader = leaders(nodes)[0]
  const follow = followers(nodes)
  t.equal(follow.length, 2, '2 followers')
  await leader.awaitEvent('commit', () => true)

  const data = []
  const count = 100
  const large = new Array(1024).fill('a').join('')
  for (let i = 0; i < count; i++) {
    data.push(toBuf({ i, large }))
  }

  let begin = Date.now()
  for (const buf of data) {
    await leader.append(buf)
  }

  let ms = Date.now() - begin
  console.log(`done leader ${count} in ${ms}ms`)
  let seconds = ms / 1000
  let avg = (count / seconds).toFixed(1)
  console.log(`${avg} append per second`)
  avg = (avg / 1000).toFixed(2)
  console.log(`${avg} append per ms\n`)

  begin = Date.now()
  for (const buf of data) {
    await follow[0].append(buf)
  }

  ms = Date.now() - begin
  console.log(`done follower ${count} in ${ms}ms`)
  seconds = ms / 1000
  avg = (count / seconds).toFixed(1)
  console.log(`${avg} append per second`)
  avg = (avg / 1000).toFixed(2)
  console.log(`${avg} append per ms`)

  t.pass('ok')
  console.log(`\n`)
}

test('test append 100 large - nodes', (t) => testAppendLargeNodes(t, new Encoder()))
test('test append 100 large - xxhash body - nodes', (t) => testAppendLargeNodes(t, new XxHashEncoder()))
test('test append 100 large - xxhash no body - nodes', (t) => testAppendLargeNodes(t, new XxHashEncoder(false)))

async function testAppendLargeTxnNodes(t, encoder) {
  t.plan(2)
  t.teardown(() => close(nodes))
  const coms = comms()
  const nodes = connect(coms, 3)

  await reset(nodes)
  await open(nodes)
  await ready(nodes)

  const leader = leaders(nodes)[0]
  const follow = followers(nodes)
  t.equal(follow.length, 2, '2 followers')
  await leader.awaitEvent('commit', () => true)

  const data = []
  const count = 1_000
  const large = new Array(1024).fill('a').join('')
  for (let i = 0; i < count; i++) {
    data.push(toBuf({ i, large }))
  }

  let begin = Date.now()
  await leader.appendBatch(data)

  let ms = Date.now() - begin
  console.log(`done leader ${count} in ${ms}ms`)
  let seconds = ms / 1000
  let avg = (count / seconds).toFixed(1)
  console.log(`${avg} append per second`)
  avg = (avg / 1000).toFixed(2)
  console.log(`${avg} append per ms\n`)

  begin = Date.now()
  await follow[0].appendBatch(data)

  ms = Date.now() - begin
  console.log(`done follower ${count} in ${ms}ms`)
  seconds = ms / 1000
  avg = (count / seconds).toFixed(1)
  console.log(`${avg} append per second`)
  avg = (avg / 1000).toFixed(2)
  console.log(`${avg} append per ms`)

  t.pass('ok')
  console.log(`\n`)
}

test('test append 1000 large txn - nodes', (t) => testAppendLargeTxnNodes(t, new Encoder()))
test('test append 1000 large txn - xxhash body - nodes', (t) => testAppendLargeTxnNodes(t, new XxHashEncoder()))
test('test append 1000 large txn - xxhash no body - nodes', (t) => testAppendLargeTxnNodes(t, new XxHashEncoder(false)))
