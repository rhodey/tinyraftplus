const test = require('tape')
const { FsLog } = require('../src/index.js')
const { TimeoutLog } = require('../src/index.js')
const { Encoder, XxHashEncoder } = require('../src/index.js')

const toBuf = (obj) => {
  if (obj === null) { return null }
  obj = JSON.stringify(obj)
  return Buffer.from(obj, 'utf8')
}

const toObj = (buf) => {
  if (buf === null) { return null }
  return JSON.parse(buf.toString('utf8'))
}

async function testTxnAppend(t, encoder, time=false) {
  t.plan(14)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog('/tmp/', 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log

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
test('test append w/ txn - time', (t) => testTxnAppend(t, new Encoder(), true))
test('test append w/ txn - xxhash body', (t) => testTxnAppend(t, new XxHashEncoder()))
test('test append w/ txn - xxhash body - time', (t) => testTxnAppend(t, new XxHashEncoder(), true))
test('test append w/ txn - xxhash no body', (t) => testTxnAppend(t, new XxHashEncoder(false)))
test('test append w/ txn - xxhash no body - time', (t) => testTxnAppend(t, new XxHashEncoder(false), true))

async function testTxnAppendBatch(t, encoder, time=false) {
  t.plan(14)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog('/tmp/', 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log

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
test('test append batch w/ txn - time', (t) => testTxnAppendBatch(t, new Encoder(), true))
test('test append batch w/ txn - xxhash body', (t) => testTxnAppendBatch(t, new XxHashEncoder()))
test('test append batch w/ txn - xxhash body - time', (t) => testTxnAppendBatch(t, new XxHashEncoder(), true))
test('test append batch w/ txn - xxhash no body', (t) => testTxnAppendBatch(t, new XxHashEncoder(false)))
test('test append batch w/ txn - xxhash no body - time', (t) => testTxnAppendBatch(t, new XxHashEncoder(false), true))

async function testTxnWithOpenClose(t, encoder, time=false) {
  t.plan(22)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog('/tmp/', 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log

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
  log = time ? new TimeoutLog(log, {}) : log
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
test('test txn with open close - time', (t) => testTxnWithOpenClose(t, new Encoder(), true))
test('test txn with open close - xxhash body', (t) => testTxnWithOpenClose(t, new XxHashEncoder()))
test('test txn with open close - xxhash body - time', (t) => testTxnWithOpenClose(t, new XxHashEncoder(), true))
test('test txn with open close - xxhash no body', (t) => testTxnWithOpenClose(t, new XxHashEncoder(false)))
test('test txn with open close - xxhash no body - time', (t) => testTxnWithOpenClose(t, new XxHashEncoder(false), true))

async function testTxnIsBlocking(t, encoder, time=false) {
  t.plan(17)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog('/tmp/', 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log

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
test('test txn is blocking - time', (t) => testTxnIsBlocking(t, new Encoder(), true))
test('test txn is blocking - xxhash body', (t) => testTxnIsBlocking(t, new XxHashEncoder()))
test('test txn is blocking - xxhash body - time', (t) => testTxnIsBlocking(t, new XxHashEncoder(), true))
test('test txn is blocking - xxhash no body', (t) => testTxnIsBlocking(t, new XxHashEncoder(false)))
test('test txn is blocking - xxhash no body - time', (t) => testTxnIsBlocking(t, new XxHashEncoder(false), true))

async function testCloseAwaitsTxn(t, encoder, time=false) {
  t.plan(9)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog('/tmp/', 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log

  await log.del()
  await log.open()

  let data = { a: 0 }
  let txn = await log.txn()
  let seq = await txn.append(toBuf(data))
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), data, 'head = data')

  let closed = false
  const close = log.close().then(() => closed = true)

  data = { bb: 2 }
  seq = await txn.append(toBuf(data))
  t.equal(seq, 1n, 'seq = 1')
  t.equal(log.seq, 1n, 'seq = 1')
  t.deepEqual(toObj(log.head), data, 'head = data')
  t.ok(!closed, 'log not closed')

  await txn.commit()
  t.pass('commit ok')

  await close
  t.ok(closed, 'log closed')
}

test('test log close awaits txn', (t) => testCloseAwaitsTxn(t, new Encoder()))
test('test log close awaits txn - time', (t) => testCloseAwaitsTxn(t, new Encoder(), true))
test('test log close awaits txn - xxhash body', (t) => testCloseAwaitsTxn(t, new XxHashEncoder()))
test('test log close awaits txn - xxhash body - time', (t) => testCloseAwaitsTxn(t, new XxHashEncoder(), true))
test('test log close awaits txn - xxhash no body', (t) => testCloseAwaitsTxn(t, new XxHashEncoder(false)))
test('test log close awaits txn - xxhash no body - time', (t) => testCloseAwaitsTxn(t, new XxHashEncoder(false), true))

async function testTxnDoubleCommitDoubleAbort(t, encoder, time=false) {
  t.plan(12)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog('/tmp/', 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log

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
test('test txn double commit double abort - time', (t) => testTxnDoubleCommitDoubleAbort(t, new Encoder(), true))
test('test txn double commit double abort - xxhash body', (t) => testTxnDoubleCommitDoubleAbort(t, new XxHashEncoder()))
test('test txn double commit double abort - xxhash body - time', (t) => testTxnDoubleCommitDoubleAbort(t, new XxHashEncoder(), true))
test('test txn double commit double abort - xxhash no body', (t) => testTxnDoubleCommitDoubleAbort(t, new XxHashEncoder(false)))
test('test txn double commit double abort - xxhash no body - time', (t) => testTxnDoubleCommitDoubleAbort(t, new XxHashEncoder(false), true))

async function testTxnTrim(t, encoder, time=false) {
  t.plan(25)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog('/tmp/', 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log

  await log.del()
  await log.open()

  await log.trim(-1n)
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

  let txn = await log.trim(-1n, true)
  await txn.commit()
  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')

  i = 2
  await log.append(data[++i])
  t.deepEqual(toObj(log.head), toObj(data[i]), 'head = data')
  await log.append(data[++i])
  t.deepEqual(toObj(log.head), toObj(data[i]), 'head = data')

  await log.trim(0n)
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), toObj(data[3]), 'head = data')

  i = 4
  await log.append(data[++i])
  t.equal(log.seq, 1n, 'seq = 1')
  t.deepEqual(toObj(log.head), toObj(data[i]), 'head = data')
  await log.append(data[++i])
  t.equal(log.seq, 2n, 'seq = 2')
  t.deepEqual(toObj(log.head), toObj(data[i]), 'head = data')

  txn = await log.trim(1n, true)
  t.equal(log.seq, 1n, 'seq = 1')
  t.deepEqual(toObj(log.head), toObj(data[5]), 'head = data')

  i = 6
  await txn.append(data[++i])
  t.equal(log.seq, 2n, 'seq = 2')
  t.deepEqual(toObj(log.head), toObj(data[7]), 'head = data')

  await txn.commit()
  t.pass('commit ok')

  // abort fails forward
  txn = await log.trim(1n, true)
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

test('test txn trim', (t) => testTxnTrim(t, new Encoder()))
test('test txn trim - time', (t) => testTxnTrim(t, new Encoder(), true))
test('test txn trim - xxhash body', (t) => testTxnTrim(t, new XxHashEncoder()))
test('test txn trim - xxhash body - time', (t) => testTxnTrim(t, new XxHashEncoder(), true))
test('test txn trim - xxhash no body', (t) => testTxnTrim(t, new XxHashEncoder(false)))
test('test txn trim - xxhash no body - time', (t) => testTxnTrim(t, new XxHashEncoder(false), true))
