const test = require('tape')
const { FsLog } = require('../lib/fslog.js')
const { Encoder, XxHashEncoder } = require('../lib/encoders.js')
const { sleep } = require('./util.js')

const toBuf = (obj) => {
  if (obj === null) { return null }
  obj = JSON.stringify(obj)
  return Buffer.from(obj, 'utf8')
}

const toObj = (buf) => {
  if (buf === null) { return null }
  return JSON.parse(buf.toString('utf8'))
}

async function testAppendWithTxn(t, encoder) {
  t.plan(6)
  const opts = { encoder }
  let log = new FsLog('/tmp/', 'test', opts)
  t.teardown(() => log.close())

  await log.del()
  await log.open()
  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')

  let data = { a: 1 }
  let txn = await log.txn()
  let seq = await txn.append(toBuf(data))
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), data, 'head = data')

  await txn.commit()
  t.pass('commit ok')
}

test('test append w/ txn', (t) => testAppendWithTxn(t, new Encoder()))
test('test append w/ txn - xxhash body', (t) => testAppendWithTxn(t, new XxHashEncoder()))
test('test append w/ txn - xxhash no body', (t) => testAppendWithTxn(t, new XxHashEncoder(false)))

async function testTxnIsBlocking(t, encoder) {
  t.plan(15)
  const opts = { encoder }
  let log = new FsLog('/tmp/', 'test', opts)
  t.teardown(() => log.close())

  await log.del()
  await log.open()
  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')

  let data = { s: 0 }
  let txn = await log.txn()
  let seq = await txn.append(toBuf(data))
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = { s: 3 }
  const delayed1 = log.append(toBuf(data))
    .then((s) => seq = s)
    .catch((err) => t.fail(err.message))

  data = { s: 4 }
  const delayed2 = log.append(toBuf(data))
    .then((s) => seq = s)
    .catch((err) => t.fail(err.message))

  data = { s: 1 }
  seq = await txn.append(toBuf(data))
  t.equal(seq, 1n, 'seq = 1')
  t.equal(log.seq, 1n, 'seq = 1')

  data = { s: 2 }
  seq = await txn.append(toBuf(data))
  t.equal(seq, 2n, 'seq = 2')
  t.equal(log.seq, 2n, 'seq = 2')
  await txn.commit()

  await delayed1
  t.equal(seq, 3n, 'seq = 3')
  t.equal(log.seq, 3n, 'seq = 3')
  t.deepEqual(toObj(log.head), { s: 3 }, 'head = data')

  await delayed2
  t.equal(seq, 4n, 'seq = 4')
  t.equal(log.seq, 4n, 'seq = 4')
  t.deepEqual(toObj(log.head), { s: 4 }, 'head = data')
}

test('test txn is blocking', (t) => testTxnIsBlocking(t, new Encoder()))
test('test txn is blocking - xxhash body', (t) => testTxnIsBlocking(t, new XxHashEncoder()))
test('test txn is blocking - xxhash no body', (t) => testTxnIsBlocking(t, new XxHashEncoder(false)))

async function testAppendWithMixed(t, encoder) {
  t.plan(22)
  const opts = { encoder }
  let log = new FsLog('/tmp/', 'test', opts)
  t.teardown(() => log.close())

  await log.del()
  await log.open()
  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')

  // open, close same
  let data = { a: 1 }
  let seq = await log.append(toBuf(data))
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), data, 'head = data')

  let txn = await log.txn()
  data = { bb: 2 }
  seq = await txn.append(toBuf(data))
  t.equal(seq, 1n, 'seq = 1')
  t.equal(log.seq, 1n, 'seq = 1')
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = { ccc: 3 }
  seq = await txn.append(toBuf(data))
  t.equal(seq, 2n, 'seq = 2')
  t.equal(log.seq, 2n, 'seq = 2')
  t.deepEqual(toObj(log.head), data, 'head = data')
  await txn.commit()
  await log.close()

  // open, close same
  await log.open()
  t.equal(log.seq, 2n, 'seq = 2 again')
  t.deepEqual(toObj(log.head), data, 'head = data again')

  data = { d: 4 }
  seq = await log.append(toBuf(data))
  t.equal(seq, 3n, 'seq = 3')
  t.equal(log.seq, 3n, 'seq = 3')
  t.deepEqual(toObj(log.head), data, 'head = data')
  await log.close()

  // new
  log = new FsLog('/tmp/', 'test', opts)
  await log.open()
  t.equal(log.seq, 3n, 'seq = 3 again')
  t.deepEqual(toObj(log.head), data, 'head = data again')

  txn = await log.txn()
  data = { ee: 5 }
  // seq = await txn.append(toBuf(data))
  seq = await log.append(toBuf(data))
  t.equal(seq, 4n, 'seq = 4')
  t.equal(log.seq, 4n, 'seq = 4')
  t.deepEqual(toObj(log.head), data, 'head = data')

  await txn.commit()
  t.pass('commit ok')
}

// test('test append w/ mixed', (t) => testAppendWithMixed(t, new Encoder()))
// test('test append w/ mixed - xxhash body', (t) => testAppendWithMixed(t, new XxHashEncoder()))
// test('test append w/ mixed - xxhash no body', (t) => testAppendWithMixed(t, new XxHashEncoder(false)))
