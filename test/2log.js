const test = require('tape')
const { FsLog } = require('../lib/fslog.js')
const { Encoder, XxHashEncoder } = require('../lib/encoders.js')

const toBuf = (obj) => {
  if (obj === null) { return null }
  obj = JSON.stringify(obj)
  return Buffer.from(obj, 'utf8')
}

const toObj = (buf) => {
  if (buf === null) { return null }
  return JSON.parse(buf.toString('utf8'))
}

async function testAppendStartStopNew(t, encoder) {
  t.plan(26)
  const opts = { encoder }
  let log = new FsLog('/tmp/', 'test', opts)
  t.teardown(() => log.stop())

  await log.del()
  await log.start()
  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')

  // start, stop same
  let data = { a: 1 }
  let ok = await log.append(toBuf(data))
  t.equal(ok.seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = { bb: 2 }
  ok = await log.append(toBuf(data))
  t.equal(ok.seq, 1n, 'seq = 1')
  t.equal(log.seq, 1n, 'seq = 1')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = { ccc: 3 }
  ok = await log.append(toBuf(data))
  t.equal(ok.seq, 2n, 'seq = 2')
  t.equal(log.seq, 2n, 'seq = 2')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')
  await log.stop()

  // start, stop same
  await log.start()
  t.equal(log.seq, 2n, 'seq = 2 again')
  t.deepEqual(toObj(log.head), data, 'head = data again')

  data = { d: 4 }
  ok = await log.append(toBuf(data))
  t.equal(ok.seq, 3n, 'seq = 3')
  t.equal(log.seq, 3n, 'seq = 3')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')
  await log.stop()

  // new
  log = new FsLog('/tmp/', 'test', opts)
  await log.start()
  t.equal(log.seq, 3n, 'seq = 3 again')
  t.deepEqual(toObj(log.head), data, 'head = data again')

  data = { ee: 5 }
  ok = await log.append(toBuf(data))
  t.equal(ok.seq, 4n, 'seq = 4')
  t.equal(log.seq, 4n, 'seq = 4')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')
}

test('test append, stop, start, append, new, append', (t) => testAppendStartStopNew(t, new Encoder()))
test('test append, stop, start, append, new, append - xxhash body', (t) => testAppendStartStopNew(t, new XxHashEncoder()))
test('test append, stop, start, append, new, append - xxhash no body', (t) => testAppendStartStopNew(t, new XxHashEncoder(false)))

async function testAppendOneStartStop(t, encoder) {
  t.plan(12)
  t.teardown(() => log.stop())

  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.start()
  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')

  let data = { a: 1 }
  let ok = await log.append(toBuf(data))
  t.equal(ok.seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')
  await log.stop()

  await log.start()
  t.equal(log.seq, 0n, 'seq = 0 again')
  t.deepEqual(toObj(log.head), data, 'head = data again')

  data = { b: 2 }
  ok = await log.append(toBuf(data))
  t.equal(ok.seq, 1n, 'seq = 1')
  t.equal(log.seq, 1n, 'seq = 1')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')
}

test('test append one, stop, start, append', (t) => testAppendOneStartStop(t, new Encoder()))
test('test append one, stop, start, append - xxhash body', (t) => testAppendOneStartStop(t, new XxHashEncoder()))
test('test append one, stop, start, append - xxhash no body', (t) => testAppendOneStartStop(t, new XxHashEncoder(false)))

async function testRollbackFirst(t, encoder) {
  t.plan(6)
  t.teardown(() => log.stop())

  const rollbackCb = (seq) => {
    if (seq === 0n) { throw new Error('test roll') }
  }

  const opts = { encoder, rollbackCb }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.start()

  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')

  try {
    const data = { a: 1 }
    await log.append(toBuf(data))
    t.fail('no error thrown')
  } catch (err) {
    t.ok(err.message.includes('test roll'), 'error thrown')
  }

  await log.stop()
  await log.start()
  t.pass('restart ok')
  t.equal(log.seq, -1n, 'seq = -1 again')
  t.equal(log.head, null, 'head = null again')
}

test('test rollback first', (t) => testRollbackFirst(t, new Encoder()))
test('test rollback first - xxhash body', (t) => testRollbackFirst(t, new XxHashEncoder()))
test('test rollback first - xxhash no body', (t) => testRollbackFirst(t, new XxHashEncoder(false)))

async function testRollbackSecond(t, encoder) {
  t.plan(8)
  t.teardown(() => log.stop())

  const rollbackCb = (seq) => {
    if (seq === 1n) { throw new Error('test roll') }
  }

  const opts = { encoder, rollbackCb }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.start()

  const data = { a: 1 }
  const ok = await log.append(toBuf(data))
  t.equal(ok.seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')

  try {
    await log.append(toBuf({ b: 2 }))
    t.fail('no error thrown')
  } catch (err) {
    t.ok(err.message.includes('test roll'), 'error thrown')
  }

  await log.stop()
  await log.start()
  t.pass('restart ok')
  t.equal(log.seq, 0n, 'seq = 0 again')
  t.deepEqual(toObj(log.head), data, 'head = data again')
}

test('test rollback second', (t) => testRollbackSecond(t, new Encoder()))
test('test rollback second - xxhash body', (t) => testRollbackSecond(t, new XxHashEncoder()))
test('test rollback second - xxhash no body', (t) => testRollbackSecond(t, new XxHashEncoder(false)))

async function testRollbackThird(t, encoder) {
  t.plan(12)
  t.teardown(() => log.stop())

  const rollbackCb = (seq) => {
    if (seq === 2n) { throw new Error('test roll') }
  }

  const opts = { encoder, rollbackCb }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.start()

  let data = { a: 1 }
  let ok = await log.append(toBuf(data))
  t.equal(ok.seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = { b: 2 }
  ok = await log.append(toBuf(data))
  t.equal(ok.seq, 1n, 'seq = 1')
  t.equal(log.seq, 1n, 'seq = 1')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')

  try {
    await log.append(toBuf({ c: 3 }))
    t.fail('no error thrown')
  } catch (err) {
    t.ok(err.message.includes('test roll'), 'error thrown')
  }

  await log.stop()
  await log.start()
  t.pass('restart ok')
  t.equal(log.seq, 1n, 'seq = 1 again')
  t.deepEqual(toObj(log.head), data, 'head = data again')
}

test('test rollback third', (t) => testRollbackThird(t, new Encoder()))
test('test rollback third - xxhash body', (t) => testRollbackThird(t, new XxHashEncoder()))
test('test rollback third - xxhash no body', (t) => testRollbackThird(t, new XxHashEncoder(false)))

async function testTruncate(t, encoder) {
  t.plan(12)
  t.teardown(() => log.stop())

  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.start()

  await log.truncate(-1n)
  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')

  const data = []
  for (let i = 0; i < 10; i++) {
    data.push(toBuf({ i }))
  }

  let i = -1
  await log.append(data[++i])
  t.deepEqual(toObj(log.head), toObj(data[i]), 'head = data')
  await log.append(data[++i])
  t.deepEqual(toObj(log.head), toObj(data[i]), 'head = data')

  await log.truncate(-1n)
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
  t.deepEqual(toObj(log.head), toObj(data[i]), 'head = data')
  await log.append(data[++i])
  t.deepEqual(toObj(log.head), toObj(data[i]), 'head = data')
}

test('test truncate, append, truncate, append, truncate, append', (t) => testTruncate(t, new Encoder()))
test('test truncate, append, truncate, append, truncate, append - xxhash body', (t) => testTruncate(t, new XxHashEncoder()))
test('test truncate, append, truncate, append, truncate, append - xxhash no body', (t) => testTruncate(t, new XxHashEncoder(false)))

async function testTruncate2(t, encoder) {
  t.plan(4)
  t.teardown(() => log.stop())

  const rollForwardCb = (seq) => {
    if (seq === -1n) { throw new Error('test roll') }
  }

  const opts = { encoder, rollForwardCb }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.start()

  await log.truncate(-1n)
  t.pass('no error')

  await log.stop()
  await log.start()
  t.pass('restart ok')
  t.equal(log.seq, -1n, 'seq = -1 again')
  t.equal(log.head, null, 'head = null')
}

test('test log seq -1 and roll forward truncate -1', (t) => testTruncate2(t, new Encoder()))
test('test log seq -1 and roll forward truncate -1 - xxhash body', (t) => testTruncate2(t, new XxHashEncoder()))
test('test log seq -1 and roll forward truncate -1 - xxhash no body', (t) => testTruncate2(t, new XxHashEncoder(false)))

async function testTruncate3(t, encoder) {
  t.plan(4)
  t.teardown(() => log.stop())

  const rollForwardCb = (seq) => {
    if (seq === -1n) { throw new Error('test roll') }
  }

  const opts = { encoder, rollForwardCb }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.start()

  const data = []
  for (let i = 0; i < 10; i++) {
    data.push(toBuf({ i }))
  }

  await log.append(data[0])

  try {
    await log.truncate(-1n)
    t.fail('no error thrown')
  } catch (err) {
    t.ok(err.message.includes('test roll'), 'error thrown')
  }

  await log.stop()
  await log.start()
  t.pass('restart ok')
  t.equal(log.seq, -1n, 'seq = -1')
  t.deepEqual(log.head, null, 'head = data')
}

test('test log seq 0 and roll forward truncate -1', (t) => testTruncate3(t, new Encoder()))
test('test log seq 0 and roll forward truncate -1 - xxhash body', (t) => testTruncate3(t, new XxHashEncoder()))
test('test log seq 0 and roll forward truncate -1 - xxhash both no body', (t) => testTruncate3(t, new XxHashEncoder(false)))

async function testBatch(t, encoder) {
  t.plan(18)
  t.teardown(() => log.stop())

  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.start()
  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')

  let data = { a: 1 }
  let ok = await log.append(toBuf(data))
  t.equal(ok.seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = [{ bb: 2 }, { ccc: 3 }]
  ok = await log.appendBatch(data.map(toBuf))
  t.equal(ok.seq, 1n, 'seq = 1')
  t.equal(log.seq, 2n, 'seq = 2')
  t.deepEqual(ok.data.map(toObj), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data[1], 'head = data')

  data = { d: 4 }
  ok = await log.append(toBuf(data))
  t.equal(ok.seq, 3n, 'seq = 3')
  t.equal(log.seq, 3n, 'seq = 3')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = { eeee: 5 }
  ok = await log.append(toBuf(data))
  t.equal(ok.seq, 4n, 'seq = 4')
  t.equal(log.seq, 4n, 'seq = 4')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')
}

test('test append batch', (t) => testBatch(t, new Encoder()))
test('test append batch - xxhash body', (t) => testBatch(t, new XxHashEncoder()))
test('test append batch - xxhash no body', (t) => testBatch(t, new XxHashEncoder(false)))

async function testBatchStartStopNew(t, encoder) {
  t.plan(24)
  t.teardown(() => log.stop())

  const opts = { encoder }
  let log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.start()
  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')

  let data = { a: 1 }
  let ok = await log.append(toBuf(data))
  t.equal(ok.seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = [{ bb: 2 }, { ccc: 3 }]
  ok = await log.appendBatch(data.map(toBuf))
  t.equal(ok.seq, 1n, 'seq = 1')
  t.equal(log.seq, 2n, 'seq = 2')
  t.deepEqual(ok.data.map(toObj), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data[1], 'head = data')
  await log.stop()

  await log.start()
  t.equal(log.seq, 2n, 'seq = 2')
  t.deepEqual(toObj(log.head), data[1], 'head = data')
  data = { d: 4 }
  ok = await log.append(toBuf(data))
  t.equal(ok.seq, 3n, 'seq = 3')
  t.equal(log.seq, 3n, 'seq = 3')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')
  await log.stop()

  await log.start()
  t.equal(log.seq, 3n, 'seq = 3')
  t.deepEqual(toObj(log.head), data, 'head = data')
  data = [{ aaa: 5 }, { bbbb: 6 }]
  ok = await log.appendBatch(data.map(toBuf))
  t.equal(ok.seq, 4n, 'seq = 4')
  t.equal(log.seq, 5n, 'seq = 5')
  t.deepEqual(ok.data.map(toObj), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data[1], 'head = data')
  await log.stop()

  log = new FsLog('/tmp/', 'test', opts)
  await log.start()
  t.equal(log.seq, 5n, 'seq = 5')
  t.deepEqual(toObj(log.head), data[1], 'head = data')
}

test('test append batch start, stop, new', (t) => testBatchStartStopNew(t, new Encoder()))
test('test append batch start, stop, new - xxhash body', (t) => testBatchStartStopNew(t, new XxHashEncoder()))
test('test append batch start, stop, new - xxhash no body', (t) => testBatchStartStopNew(t, new XxHashEncoder(false)))

async function testBatchRollback(t, encoder) {
  t.plan(6)
  t.teardown(() => log.stop())

  const rollbackCb = (seq) => {
    if (seq === 0n) { throw new Error('test roll') }
  }

  const opts = { encoder, rollbackCb }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.start()

  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')

  try {
    const data = [{ a: 1 }]
    await log.appendBatch(data.map(toBuf))
    t.fail('no error thrown')
  } catch (err) {
    t.ok(err.message.includes('test roll'), 'error thrown')
  }

  await log.stop()
  await log.start()
  t.pass('restart ok')
  t.equal(log.seq, -1n, 'seq = -1 again')
  t.equal(log.head, null, 'head = null again')
}

test('test rollback batch first', (t) => testBatchRollback(t, new Encoder()))
test('test rollback batch first - xxhash body', (t) => testBatchRollback(t, new XxHashEncoder()))
test('test rollback batch first - xxhash no body', (t) => testBatchRollback(t, new XxHashEncoder(false)))

async function testBatchRollback2(t, encoder) {
  t.plan(8)
  t.teardown(() => log.stop())

  const rollbackCb = (seq) => {
    if (seq === 1n) { throw new Error('test roll') }
  }

  const opts = { encoder, rollbackCb }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.start()

  const data = [{ a: 1 }]
  const ok = await log.appendBatch(data.map(toBuf))
  t.equal(ok.seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(ok.data.map(toObj), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data[0], 'head = data')

  try {
    const more = [{ bb: 2 }]
    await log.appendBatch(more.map(toBuf))
    t.fail('no error thrown')
  } catch (err) {
    t.ok(err.message.includes('test roll'), 'error thrown')
  }

  await log.stop()
  await log.start()
  t.pass('restart ok')
  t.equal(log.seq, 0n, 'seq = 0 again')
  t.deepEqual(toObj(log.head), data[0], 'head = data')
}

test('test rollback batch second', (t) => testBatchRollback2(t, new Encoder()))
test('test rollback batch second - xxhash body', (t) => testBatchRollback2(t, new XxHashEncoder()))
test('test rollback batch second - xxhash no body', (t) => testBatchRollback2(t, new XxHashEncoder(false)))

async function testBatchRollback3(t, encoder) {
  t.plan(12)
  t.teardown(() => log.stop())

  const rollbackCb = (seq) => {
    if (seq === 3n) { throw new Error('test roll') }
  }

  const opts = { encoder, rollbackCb }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.start()

  let data = { a: 1 }
  let ok = await log.append(toBuf(data))
  t.equal(ok.seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = [{ bb: 2 }, { ccc: 3 }]
  ok = await log.appendBatch(data.map(toBuf))
  t.equal(ok.seq, 1n, 'seq = 1')
  t.equal(log.seq, 2n, 'seq = 2')
  t.deepEqual(ok.data.map(toObj), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data[1], 'head = data')

  try {
    const more = [{ dddd: 4 }]
    await log.appendBatch(more.map(toBuf))
    t.fail('no error thrown')
  } catch (err) {
    t.ok(err.message.includes('test roll'), 'error thrown')
  }

  await log.stop()
  await log.start()
  t.pass('restart ok')
  t.equal(log.seq, 2n, 'seq = 2 again')
  t.deepEqual(toObj(log.head), data[1], 'head = data')
}

test('test rollback batch third', (t) => testBatchRollback3(t, new Encoder()))
test('test rollback batch third - xxhash body', (t) => testBatchRollback3(t, new XxHashEncoder()))
test('test rollback batch third - xxhash no body', (t) => testBatchRollback3(t, new XxHashEncoder(false)))

async function testBatchTruncate(t, encoder) {
  t.plan(2)
  t.teardown(() => log.stop())

  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.start()

  const data = [{ a: 1 }, { bb: 2 }]
  await log.appendBatch(data.map(toBuf))
  await log.truncate(-1n)

  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')
}

test('test append batch then truncate -1', (t) => testBatchTruncate(t, new Encoder()))
test('test append batch then truncate -1 - xxhash body', (t) => testBatchTruncate(t, new XxHashEncoder()))
test('test append batch then truncate -1 - xxhash no body', (t) => testBatchTruncate(t, new XxHashEncoder(false)))

async function testBatchTruncate2(t, encoder) {
  t.plan(4)
  t.teardown(() => log.stop())

  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.start()

  let data = [{ a: 1 }, { bb: 2 }]
  await log.appendBatch(data.map(toBuf))
  await log.truncate(0n)

  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), data[0], 'head = data')

  data = [{ ccc: 3 }]
  await log.appendBatch(data.map(toBuf))
  t.equal(log.seq, 1n, 'seq = 1')
  t.deepEqual(toObj(log.head), data[0], 'head = data')
}

test('test append batch then truncate 0', (t) => testBatchTruncate2(t, new Encoder()))
test('test append batch then truncate 0 - xxhash body', (t) => testBatchTruncate2(t, new XxHashEncoder()))
test('test append batch then truncate 0 - xxhash no body', (t) => testBatchTruncate2(t, new XxHashEncoder(false)))

async function testBatchTruncate3(t, encoder) {
  t.plan(6)
  t.teardown(() => log.stop())

  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.start()

  let data = [{ a: 1 }, { bb: 2 }]
  await log.appendBatch(data.map(toBuf))
  await log.truncate(0n)

  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), data[0], 'head = data')

  data = [{ ccc: 3 }, { dddd: 4 }]
  await log.appendBatch(data.map(toBuf))
  t.equal(log.seq, 2n, 'seq = 2')
  t.deepEqual(toObj(log.head), data[1], 'head = data')

  await log.truncate(1n)
  t.equal(log.seq, 1n, 'seq = 1')
  t.deepEqual(toObj(log.head), data[0], 'head = data')
}

test('test append batch then truncate 1', (t) => testBatchTruncate3(t, new Encoder()))
test('test append batch then truncate 1 - xxhash body', (t) => testBatchTruncate3(t, new XxHashEncoder()))
test('test append batch then truncate 1 - xxhash no body', (t) => testBatchTruncate3(t, new XxHashEncoder(false)))
