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

async function testTxnAppend(t, encoder) {
  t.plan(14)
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
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = { b: 2 }
  txn = await log.txn()
  seq = await txn.append(toBuf(data))
  t.equal(seq, 1n, 'seq = 1')
  t.equal(log.seq, 1n, 'seq = 1')
  t.deepEqual(toObj(log.head), data, 'head = data')

  await txn.abort()
  t.pass('abort ok')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), { a: 1 }, 'head = data')
}

test('test append w/ txn', (t) => testTxnAppend(t, new Encoder()))
test('test append w/ txn - xxhash body', (t) => testTxnAppend(t, new XxHashEncoder()))
test('test append w/ txn - xxhash no body', (t) => testTxnAppend(t, new XxHashEncoder(false)))

async function testTxnAppendBatch(t, encoder) {
  t.plan(14)
  const opts = { encoder }
  let log = new FsLog('/tmp/', 'test', opts)
  t.teardown(() => log.close())

  await log.del()
  await log.open()
  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')

  let data = [{ a: 1 }, { bb: 2 }]
  let txn = await log.txn()
  let seq = await txn.appendBatch(data.map(toBuf))
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 1n, 'seq = 1')
  t.deepEqual(toObj(log.head), data[1], 'head = data')

  await txn.commit()
  t.pass('commit ok')
  t.equal(log.seq, 1n, 'seq = 1')
  t.deepEqual(toObj(log.head), data[1], 'head = data')

  data = [{ ccc: 3 }]
  txn = await log.txn()
  seq = await txn.appendBatch(data.map(toBuf))
  t.equal(seq, 2n, 'seq = 2')
  t.equal(log.seq, 2n, 'seq = 2')
  t.deepEqual(toObj(log.head), data[0], 'head = data')

  await txn.abort()
  t.pass('abort ok')
  t.equal(log.seq, 1n, 'seq = 1')
  t.deepEqual(toObj(log.head), { bb: 2 }, 'head = data')
}

test('test append batch w/ txn', (t) => testTxnAppendBatch(t, new Encoder()))
test('test append batch w/ txn - xxhash body', (t) => testTxnAppendBatch(t, new XxHashEncoder()))
test('test append batch w/ txn - xxhash no body', (t) => testTxnAppendBatch(t, new XxHashEncoder(false)))

async function testTxnWithOpenClose(t, encoder) {
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

  data = { bb: 2 }
  let txn = await log.txn()
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
  seq = await txn.append(toBuf(data))
  t.equal(seq, 4n, 'seq = 4')
  t.equal(log.seq, 4n, 'seq = 4')
  t.deepEqual(toObj(log.head), data, 'head = data')

  await txn.commit()
  t.pass('commit ok')
}

test('test txn with open close', (t) => testTxnWithOpenClose(t, new Encoder()))
test('test txn with open close - xxhash body', (t) => testTxnWithOpenClose(t, new XxHashEncoder()))
test('test txn with open close - xxhash no body', (t) => testTxnWithOpenClose(t, new XxHashEncoder(false)))

async function testTxnIsBlocking(t, encoder) {
  t.plan(17)
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
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = { s: 2 }
  seq = await txn.append(toBuf(data))
  t.equal(seq, 2n, 'seq = 2')
  t.equal(log.seq, 2n, 'seq = 2')
  t.deepEqual(toObj(log.head), data, 'head = data')

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

async function testTxnCloseOnLogClose(t, encoder) {
  t.plan(10)
  const opts = { encoder }
  let log = new FsLog('/tmp/', 'test', opts)
  t.teardown(() => log.close())

  await log.del()
  await log.open()

  let data = { a: 0 }
  let txn = await log.txn()
  let seq = await txn.append(toBuf(data))
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), data, 'head = data')
  await log.close()
  await log.open()

  try {
    await txn.append(toBuf(data))
    t.fail('txn not closed')
  } catch (err) {
    t.ok(err.message.includes('was closed'))
  }

  data = { b: 0 }
  txn = await log.txn()
  seq = await txn.append(toBuf(data))
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), data, 'head = data')

  await txn.commit()
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), data, 'head = data')
}

test('test txn close on log close', (t) => testTxnCloseOnLogClose(t, new Encoder()))
test('test txn close on log close - xxhash body', (t) => testTxnCloseOnLogClose(t, new XxHashEncoder()))
test('test txn close on log close - xxhash no body', (t) => testTxnCloseOnLogClose(t, new XxHashEncoder(false)))

async function testTxnDoubleCommitDoubleAbort(t, encoder) {
  t.plan(12)
  const opts = { encoder }
  let log = new FsLog('/tmp/', 'test', opts)
  t.teardown(() => log.close())

  await log.del()
  await log.open()

  let data = { a: 0 }
  let txn = await log.txn()
  let seq = await txn.append(toBuf(data))
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), data, 'head = data')

  await txn.commit()
  t.pass('commit ok')

  try {
    await txn.commit()
    t.fail('no error')
  } catch (err) {
    t.ok(err.message.includes('already commit or abort'))
  }

  data = { b: 1 }
  txn = await log.txn()
  seq = await txn.append(toBuf(data))
  t.equal(seq, 1n, 'seq = 1')
  t.equal(log.seq, 1n, 'seq = 1')
  t.deepEqual(toObj(log.head), data, 'head = data')

  await txn.abort()
  t.pass('abort ok')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), { a: 0 }, 'head = data')

  try {
    await txn.abort()
    t.fail('no error')
  } catch (err) {
    t.ok(err.message.includes('already commit or abort'))
  }
}

test('test txn double commit double abort', (t) => testTxnDoubleCommitDoubleAbort(t, new Encoder()))
test('test txn double commit double abort - xxhash body', (t) => testTxnDoubleCommitDoubleAbort(t, new XxHashEncoder()))
test('test txn double commit double abort - xxhash no body', (t) => testTxnDoubleCommitDoubleAbort(t, new XxHashEncoder(false)))

async function testTxnTruncate(t, encoder) {
  t.plan(25)
  t.teardown(() => log.close())

  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.open()

  await log.truncate(-1n)
  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')

  const data = []
  for (let i = 0; i < 10; i++) {
    data.push(toBuf({ i }))
  }

  let i = 0
  await log.append(data[i])
  t.deepEqual(toObj(log.head), toObj(data[i]), 'head = data')
  await log.append(data[++i])
  t.deepEqual(toObj(log.head), toObj(data[i]), 'head = data')

  let txn = await log.truncate(-1n, true)
  await txn.commit()
  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')

  i = 2
  await log.append(data[++i])
  t.deepEqual(toObj(log.head), toObj(data[i]), 'head = data')
  await log.append(data[++i])
  t.deepEqual(toObj(log.head), toObj(data[i]), 'head = data')

  await log.truncate(0n)
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), toObj(data[3]), 'head = data')

  i = 4
  await log.append(data[++i])
  t.equal(log.seq, 1n, 'seq = 1')
  t.deepEqual(toObj(log.head), toObj(data[i]), 'head = data')
  await log.append(data[++i])
  t.equal(log.seq, 2n, 'seq = 2')
  t.deepEqual(toObj(log.head), toObj(data[i]), 'head = data')

  txn = await log.truncate(1n, true)
  t.equal(log.seq, 1n, 'seq = 1')
  t.deepEqual(toObj(log.head), toObj(data[5]), 'head = data')

  i = 6
  await txn.append(data[++i])
  t.equal(log.seq, 2n, 'seq = 2')
  t.deepEqual(toObj(log.head), toObj(data[7]), 'head = data')

  await txn.commit()
  t.pass('commit ok')

  // abort fails forward
  txn = await log.truncate(1n, true)
  t.equal(log.seq, 1n, 'seq = 1')
  t.deepEqual(toObj(log.head), toObj(data[5]), 'head = data')

  i = 7
  await txn.append(data[++i])
  t.equal(log.seq, 2n, 'seq = 2')
  t.deepEqual(toObj(log.head), toObj(data[8]), 'head = data')

  await txn.abort()
  t.equal(log.seq, 1n, 'seq = 1')
  t.deepEqual(toObj(log.head), toObj(data[5]), 'head = data')
}

test('test txn truncate', (t) => testTxnTruncate(t, new Encoder()))
test('test txn truncate - xxhash body', (t) => testTxnTruncate(t, new XxHashEncoder()))
test('test txn truncate - xxhash no body', (t) => testTxnTruncate(t, new XxHashEncoder(false)))
