const test = require('tape')
const { FsLog } = require('../src/index.js')
const { Encoder, XxHashEncoder } = require('../src/index.js')

const DIR = process.env.TEST_DIR ?? '/tmp/'

const toBuf = (obj) => {
  if (obj === null) { return null }
  obj = JSON.stringify(obj)
  return Buffer.from(obj, 'utf8')
}

const toObj = (buf) => {
  if (buf === null) { return null }
  return JSON.parse(buf.toString('utf8'))
}

async function testAppendOpenCloseNew(t, encoder) {
  t.plan(21)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog(DIR, 'test', opts)

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
  seq = await log.append(toBuf(data))
  t.equal(seq, 1n, 'seq = 1')
  t.equal(log.seq, 1n, 'seq = 1')
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = { ccc: 3 }
  seq = await log.append(toBuf(data))
  t.equal(seq, 2n, 'seq = 2')
  t.equal(log.seq, 2n, 'seq = 2')
  t.deepEqual(toObj(log.head), data, 'head = data')
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
  log = new FsLog(DIR, 'test', opts)
  await log.open()
  t.equal(log.seq, 3n, 'seq = 3 again')
  t.deepEqual(toObj(log.head), data, 'head = data again')

  data = { ee: 5 }
  seq = await log.append(toBuf(data))
  t.equal(seq, 4n, 'seq = 4')
  t.equal(log.seq, 4n, 'seq = 4')
  t.deepEqual(toObj(log.head), data, 'head = data')
}

test('test append, open, close, new', (t) => testAppendOpenCloseNew(t, new Encoder()))
test('test append, open, close, new - xxhash body', (t) => testAppendOpenCloseNew(t, new XxHashEncoder()))
test('test append, open, close, new - xxhash no body', (t) => testAppendOpenCloseNew(t, new XxHashEncoder(false)))

async function testAppendOneCloseOpen(t, encoder) {
  t.plan(10)
  t.teardown(() => log.close())

  const opts = { encoder }
  const log = new FsLog(DIR, 'test', opts)

  await log.del()
  await log.open()

  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')

  let data = { a: 1 }
  let seq = await log.append(toBuf(data))
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), data, 'head = data')
  await log.close()

  await log.open()
  t.equal(log.seq, 0n, 'seq = 0 again')
  t.deepEqual(toObj(log.head), data, 'head = data again')

  data = { b: 2 }
  seq = await log.append(toBuf(data))
  t.equal(seq, 1n, 'seq = 1')
  t.equal(log.seq, 1n, 'seq = 1')
  t.deepEqual(toObj(log.head), data, 'head = data')
}

test('test append one, close, open, append', (t) => testAppendOneCloseOpen(t, new Encoder()))
test('test append one, close, open, append - xxhash body', (t) => testAppendOneCloseOpen(t, new XxHashEncoder()))
test('test append one, close, open, append - xxhash no body', (t) => testAppendOneCloseOpen(t, new XxHashEncoder(false)))

async function testRollbackFirst(t, encoder) {
  t.plan(5)
  t.teardown(() => log.close())

  const rollbackCb = (seq) => {
    if (seq === 0n) { throw new Error('test roll') }
  }

  const opts = { encoder, rollbackCb }
  const log = new FsLog(DIR, 'test', opts)

  await log.del()
  await log.open()

  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')

  try {
    const data = { a: 1 }
    await log.append(toBuf(data))
    t.fail('no error thrown')
  } catch (err) {
    t.ok(err.message.includes('test roll'), 'error thrown')
  }

  t.equal(log.seq, -1n, 'seq = -1 again')
  t.equal(log.head, null, 'head = null again')
}

test('test rollback first', (t) => testRollbackFirst(t, new Encoder()))
test('test rollback first - xxhash body', (t) => testRollbackFirst(t, new XxHashEncoder()))
test('test rollback first - xxhash no body', (t) => testRollbackFirst(t, new XxHashEncoder(false)))

async function testRollbackSecond(t, encoder) {
  t.plan(7)
  t.teardown(() => log.close())

  const rollbackCb = (seq) => {
    if (seq === 1n) { throw new Error('test roll') }
  }

  const opts = { encoder, rollbackCb }
  const log = new FsLog(DIR, 'test', opts)

  await log.del()
  await log.open()

  const data = { a: 1 }
  const seq = await log.append(toBuf(data))
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), data, 'head = data')

  try {
    await log.append(toBuf({ b: 2 }))
    t.fail('no error thrown')
  } catch (err) {
    t.ok(err.message.includes('test roll'), 'error thrown')
  }

  await log.close()
  await log.open()
  t.pass('restart ok')
  t.equal(log.seq, 0n, 'seq = 0 again')
  t.deepEqual(toObj(log.head), data, 'head = data again')
}

test('test rollback second', (t) => testRollbackSecond(t, new Encoder()))
test('test rollback second - xxhash body', (t) => testRollbackSecond(t, new XxHashEncoder()))
test('test rollback second - xxhash no body', (t) => testRollbackSecond(t, new XxHashEncoder(false)))

async function testRollbackThird(t, encoder) {
  t.plan(9)
  t.teardown(() => log.close())

  const rollbackCb = (seq) => {
    if (seq === 2n) { throw new Error('test roll') }
  }

  const opts = { encoder, rollbackCb }
  const log = new FsLog(DIR, 'test', opts)

  await log.del()
  await log.open()

  let data = { a: 1 }
  let seq = await log.append(toBuf(data))
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = { b: 2 }
  seq = await log.append(toBuf(data))
  t.equal(seq, 1n, 'seq = 1')
  t.equal(log.seq, 1n, 'seq = 1')
  t.deepEqual(toObj(log.head), data, 'head = data')

  try {
    await log.append(toBuf({ c: 3 }))
    t.fail('no error thrown')
  } catch (err) {
    t.ok(err.message.includes('test roll'), 'error thrown')
  }

  t.equal(log.seq, 1n, 'seq = 1 again')
  t.deepEqual(toObj(log.head), data, 'head = data again')
}

test('test rollback third', (t) => testRollbackThird(t, new Encoder()))
test('test rollback third - xxhash body', (t) => testRollbackThird(t, new XxHashEncoder()))
test('test rollback third - xxhash no body', (t) => testRollbackThird(t, new XxHashEncoder(false)))

async function testTrim(t, encoder) {
  t.plan(12)
  t.teardown(() => log.close())

  const opts = { encoder }
  const log = new FsLog(DIR, 'test', opts)

  await log.del()
  await log.open()

  await log.trim(-1n)
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

  await log.trim(-1n)
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
  t.deepEqual(toObj(log.head), toObj(data[i]), 'head = data')
  await log.append(data[++i])
  t.deepEqual(toObj(log.head), toObj(data[i]), 'head = data')
}

test('test trim', (t) => testTrim(t, new Encoder()))
test('test trim - xxhash body', (t) => testTrim(t, new XxHashEncoder()))
test('test trim - xxhash no body', (t) => testTrim(t, new XxHashEncoder(false)))

async function testTrim2(t, encoder) {
  t.plan(4)
  t.teardown(() => log.close())

  const opts = { encoder }
  const log = new FsLog(DIR, 'test', opts)

  await log.del()
  await log.open()

  await log.trim(-1n)
  t.pass('no error')

  await log.close()
  await log.open()
  t.pass('restart ok')
  t.equal(log.seq, -1n, 'seq = -1 again')
  t.equal(log.head, null, 'head = null')
}

test('test trim empty -1', (t) => testTrim2(t, new Encoder()))
test('test trim empty -1 - xxhash body', (t) => testTrim2(t, new XxHashEncoder()))
test('test trim empty -1 - xxhash no body', (t) => testTrim2(t, new XxHashEncoder(false)))

async function testTrim3(t, encoder) {
  t.plan(3)
  t.teardown(() => log.close())

  const rollForwardCb = (seq) => {
    if (seq === -1n) { throw new Error('test roll') }
  }

  const opts = { encoder, rollForwardCb }
  const log = new FsLog(DIR, 'test', opts)

  await log.del()
  await log.open()

  await log.append(toBuf({ rm: 1 }))

  try {
    await log.trim(-1n)
    t.fail('no error thrown')
  } catch (err) {
    t.ok(err.message.includes('test roll'), 'error thrown')
  }

  t.equal(log.seq, -1n, 'seq = -1')
  t.deepEqual(log.head, null, 'head = data')
}

test('test trim 0 -1', (t) => testTrim3(t, new Encoder()))
test('test trim 0 -1 - xxhash body', (t) => testTrim3(t, new XxHashEncoder()))
test('test trim 0 -1 - xxhash both no body', (t) => testTrim3(t, new XxHashEncoder(false)))

async function testBatch(t, encoder) {
  t.plan(14)
  t.teardown(() => log.close())

  const opts = { encoder }
  const log = new FsLog(DIR, 'test', opts)

  await log.del()
  await log.open()

  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')

  let data = { a: 1 }
  let seq = await log.append(toBuf(data))
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = [{ bb: 2 }, { ccc: 3 }]
  seq = await log.appendBatch(data.map(toBuf))
  t.equal(seq, 1n, 'seq = 1')
  t.equal(log.seq, 2n, 'seq = 2')
  t.deepEqual(toObj(log.head), data[1], 'head = data')

  data = { d: 4 }
  seq = await log.append(toBuf(data))
  t.equal(seq, 3n, 'seq = 3')
  t.equal(log.seq, 3n, 'seq = 3')
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = { eeee: 5 }
  seq = await log.append(toBuf(data))
  t.equal(seq, 4n, 'seq = 4')
  t.equal(log.seq, 4n, 'seq = 4')
  t.deepEqual(toObj(log.head), data, 'head = data')
}

test('test append batch', (t) => testBatch(t, new Encoder()))
test('test append batch - xxhash body', (t) => testBatch(t, new XxHashEncoder()))
test('test append batch - xxhash no body', (t) => testBatch(t, new XxHashEncoder(false)))

async function testBatchOpenCloseNew(t, encoder) {
  t.plan(20)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog(DIR, 'test', opts)

  await log.del()
  await log.open()

  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')

  let data = { a: 1 }
  let seq = await log.append(toBuf(data))
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = [{ bb: 2 }, { ccc: 3 }]
  seq = await log.appendBatch(data.map(toBuf))
  t.equal(seq, 1n, 'seq = 1')
  t.equal(log.seq, 2n, 'seq = 2')
  t.deepEqual(toObj(log.head), data[1], 'head = data')
  await log.close()

  await log.open()
  t.equal(log.seq, 2n, 'seq = 2')
  t.deepEqual(toObj(log.head), data[1], 'head = data')
  data = { d: 4 }
  seq = await log.append(toBuf(data))
  t.equal(seq, 3n, 'seq = 3')
  t.equal(log.seq, 3n, 'seq = 3')
  t.deepEqual(toObj(log.head), data, 'head = data')
  await log.close()

  await log.open()
  t.equal(log.seq, 3n, 'seq = 3')
  t.deepEqual(toObj(log.head), data, 'head = data')
  data = [{ aaa: 5 }, { bbbb: 6 }]
  seq = await log.appendBatch(data.map(toBuf))
  t.equal(seq, 4n, 'seq = 4')
  t.equal(log.seq, 5n, 'seq = 5')
  t.deepEqual(toObj(log.head), data[1], 'head = data')
  await log.close()

  log = new FsLog(DIR, 'test', opts)
  await log.open()
  t.equal(log.seq, 5n, 'seq = 5')
  t.deepEqual(toObj(log.head), data[1], 'head = data')
}

test('test batch open, close, new', (t) => testBatchOpenCloseNew(t, new Encoder()))
test('test batch open, close, new - xxhash body', (t) => testBatchOpenCloseNew(t, new XxHashEncoder()))
test('test batch open, close, new - xxhash no body', (t) => testBatchOpenCloseNew(t, new XxHashEncoder(false)))

async function testBatchRollback(t, encoder) {
  t.plan(5)
  t.teardown(() => log.close())

  const rollbackCb = (seq) => {
    if (seq === 0n) { throw new Error('test roll') }
  }

  const opts = { encoder, rollbackCb }
  const log = new FsLog(DIR, 'test', opts)

  await log.del()
  await log.open()

  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')

  try {
    const data = [{ a: 1 }]
    await log.appendBatch(data.map(toBuf))
    t.fail('no error thrown')
  } catch (err) {
    t.ok(err.message.includes('test roll'), 'error thrown')
  }

  t.equal(log.seq, -1n, 'seq = -1 again')
  t.equal(log.head, null, 'head = null again')
}

test('test rollback batch first', (t) => testBatchRollback(t, new Encoder()))
test('test rollback batch first - xxhash body', (t) => testBatchRollback(t, new XxHashEncoder()))
test('test rollback batch first - xxhash no body', (t) => testBatchRollback(t, new XxHashEncoder(false)))

async function testBatchRollback2(t, encoder) {
  t.plan(6)
  t.teardown(() => log.close())

  const rollbackCb = (seq) => {
    if (seq === 1n) { throw new Error('test roll') }
  }

  const opts = { encoder, rollbackCb }
  const log = new FsLog(DIR, 'test', opts)

  await log.del()
  await log.open()

  const data = [{ a: 1 }]
  const seq = await log.appendBatch(data.map(toBuf))
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), data[0], 'head = data')

  try {
    const more = [{ bb: 2 }]
    await log.appendBatch(more.map(toBuf))
    t.fail('no error thrown')
  } catch (err) {
    t.ok(err.message.includes('test roll'), 'error thrown')
  }

  t.equal(log.seq, 0n, 'seq = 0 again')
  t.deepEqual(toObj(log.head), data[0], 'head = data')
}

test('test rollback batch second', (t) => testBatchRollback2(t, new Encoder()))
test('test rollback batch second - xxhash body', (t) => testBatchRollback2(t, new XxHashEncoder()))
test('test rollback batch second - xxhash no body', (t) => testBatchRollback2(t, new XxHashEncoder(false)))

async function testBatchRollback3(t, encoder) {
  t.plan(9)
  t.teardown(() => log.close())

  const rollbackCb = (seq) => {
    if (seq === 3n) { throw new Error('test roll') }
  }

  const opts = { encoder, rollbackCb }
  const log = new FsLog(DIR, 'test', opts)

  await log.del()
  await log.open()

  let data = { a: 1 }
  let seq = await log.append(toBuf(data))
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = [{ bb: 2 }, { ccc: 3 }]
  seq = await log.appendBatch(data.map(toBuf))
  t.equal(seq, 1n, 'seq = 1')
  t.equal(log.seq, 2n, 'seq = 2')
  t.deepEqual(toObj(log.head), data[1], 'head = data')

  try {
    const more = [{ dddd: 4 }]
    await log.appendBatch(more.map(toBuf))
    t.fail('no error thrown')
  } catch (err) {
    t.ok(err.message.includes('test roll'), 'error thrown')
  }

  t.equal(log.seq, 2n, 'seq = 2 again')
  t.deepEqual(toObj(log.head), data[1], 'head = data')
}

test('test rollback batch third', (t) => testBatchRollback3(t, new Encoder()))
test('test rollback batch third - xxhash body', (t) => testBatchRollback3(t, new XxHashEncoder()))
test('test rollback batch third - xxhash no body', (t) => testBatchRollback3(t, new XxHashEncoder(false)))

async function testBatchTrim(t, encoder) {
  t.plan(2)
  t.teardown(() => log.close())

  const opts = { encoder }
  const log = new FsLog(DIR, 'test', opts)

  await log.del()
  await log.open()

  const data = [{ a: 1 }, { bb: 2 }]
  await log.appendBatch(data.map(toBuf))
  await log.trim(-1n)

  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')
}

test('test batch trim -1', (t) => testBatchTrim(t, new Encoder()))
test('test batch trim -1 - xxhash body', (t) => testBatchTrim(t, new XxHashEncoder()))
test('test batch trim -1 - xxhash no body', (t) => testBatchTrim(t, new XxHashEncoder(false)))

async function testBatchTrim2(t, encoder) {
  t.plan(4)
  t.teardown(() => log.close())

  const opts = { encoder }
  const log = new FsLog(DIR, 'test', opts)

  await log.del()
  await log.open()

  let data = [{ a: 1 }, { bb: 2 }]
  await log.appendBatch(data.map(toBuf))
  await log.trim(0n)

  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), data[0], 'head = data')

  data = [{ ccc: 3 }]
  await log.appendBatch(data.map(toBuf))
  t.equal(log.seq, 1n, 'seq = 1')
  t.deepEqual(toObj(log.head), data[0], 'head = data')
}

test('test batch trim 0', (t) => testBatchTrim2(t, new Encoder()))
test('test batch trim 0 - xxhash body', (t) => testBatchTrim2(t, new XxHashEncoder()))
test('test batch trim 0 - xxhash no body', (t) => testBatchTrim2(t, new XxHashEncoder(false)))

async function testBatchTrim3(t, encoder) {
  t.plan(6)
  t.teardown(() => log.close())

  const opts = { encoder }
  const log = new FsLog(DIR, 'test', opts)

  await log.del()
  await log.open()

  let data = [{ a: 1 }, { bb: 2 }]
  await log.appendBatch(data.map(toBuf))
  await log.trim(0n)

  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), data[0], 'head = data')

  data = [{ ccc: 3 }, { dddd: 4 }]
  await log.appendBatch(data.map(toBuf))
  t.equal(log.seq, 2n, 'seq = 2')
  t.deepEqual(toObj(log.head), data[1], 'head = data')

  await log.trim(1n)
  t.equal(log.seq, 1n, 'seq = 1')
  t.deepEqual(toObj(log.head), data[0], 'head = data')
}

test('test batch trim 1', (t) => testBatchTrim3(t, new Encoder()))
test('test batch trim 1 - xxhash body', (t) => testBatchTrim3(t, new XxHashEncoder()))
test('test batch trim 1 - xxhash no body', (t) => testBatchTrim3(t, new XxHashEncoder(false)))

async function testAppendBufEmpty(t, encoder) {
  t.plan(21)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog(DIR, 'test', opts)

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

  data = Buffer.alloc(0)
  seq = await log.append(data)
  t.equal(seq, 1n, 'seq = 1')
  t.equal(log.seq, 1n, 'seq = 1')
  t.ok(log.head.equals(data), 'head = data')

  data = Buffer.alloc(0)
  seq = await log.append(data)
  t.equal(seq, 2n, 'seq = 2')
  t.equal(log.seq, 2n, 'seq = 2')
  t.ok(log.head.equals(data), 'head = data')
  await log.close()

  // open, close same
  await log.open()
  t.equal(log.seq, 2n, 'seq = 2 again')
  t.ok(log.head.equals(data), 'head = data again')

  data = { d: 4 }
  seq = await log.append(toBuf(data))
  t.equal(seq, 3n, 'seq = 3')
  t.equal(log.seq, 3n, 'seq = 3')
  t.deepEqual(toObj(log.head), data, 'head = data')
  await log.close()

  // new
  log = new FsLog(DIR, 'test', opts)
  await log.open()
  t.equal(log.seq, 3n, 'seq = 3 again')
  t.deepEqual(toObj(log.head), data, 'head = data again')

  data = Buffer.alloc(0)
  seq = await log.append(data)
  t.equal(seq, 4n, 'seq = 4')
  t.equal(log.seq, 4n, 'seq = 4')
  t.ok(log.head.equals(data), 'head = data')
}

test('test append buf empty', (t) => testAppendBufEmpty(t, new Encoder()))
test('test append buf empty - xxhash body', (t) => testAppendBufEmpty(t, new XxHashEncoder()))
test('test append buf empty - xxhash no body', (t) => testAppendBufEmpty(t, new XxHashEncoder(false)))

async function testAppendBatchBufEmpty(t, encoder) {
  t.plan(20)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog(DIR, 'test', opts)

  await log.del()
  await log.open()

  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')

  let data = { a: 1 }
  let seq = await log.append(toBuf(data))
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = [Buffer.alloc(0), Buffer.alloc(0)]
  seq = await log.appendBatch(data)
  t.equal(seq, 1n, 'seq = 1')
  t.equal(log.seq, 2n, 'seq = 2')
  t.ok(log.head.equals(data[1]), 'head = data')
  await log.close()

  await log.open()
  t.equal(log.seq, 2n, 'seq = 2')
  t.ok(log.head.equals(data[1]), 'head = data')
  data = { d: 4 }
  seq = await log.append(toBuf(data))
  t.equal(seq, 3n, 'seq = 3')
  t.equal(log.seq, 3n, 'seq = 3')
  t.deepEqual(toObj(log.head), data, 'head = data')
  await log.close()

  await log.open()
  t.equal(log.seq, 3n, 'seq = 3')
  t.deepEqual(toObj(log.head), data, 'head = data')
  data = [Buffer.alloc(0), Buffer.alloc(0)]
  seq = await log.appendBatch(data)
  t.equal(seq, 4n, 'seq = 4')
  t.equal(log.seq, 5n, 'seq = 5')
  t.ok(log.head.equals(data[1]), 'head = data')
  await log.close()

  log = new FsLog(DIR, 'test', opts)
  await log.open()
  t.equal(log.seq, 5n, 'seq = 5')
  t.ok(log.head.equals(data[1]), 'head = data')
}

test('test batch buf empty', (t) => testAppendBatchBufEmpty(t, new Encoder()))
test('test batch buf empty - xxhash body', (t) => testAppendBatchBufEmpty(t, new XxHashEncoder()))
test('test batch buf empty - xxhash no body', (t) => testAppendBatchBufEmpty(t, new XxHashEncoder(false)))

async function testDoubleOpen(t, encoder) {
  t.plan(1)
  t.teardown(() => log.close())
  const opts = { encoder }
  const log = new FsLog(DIR, 'test', opts)
  await log.del()
  const p1 = log.open()
  const p2 = log.open()
  t.ok(p1 === p2, 'p1 = p2')
}

test('test double open', (t) => testDoubleOpen(t, new Encoder()))

async function testDoubleClose(t, encoder) {
  t.plan(1)
  t.teardown(() => log.close())
  const opts = { encoder }
  const log = new FsLog(DIR, 'test', opts)
  await log.del()
  await log.open()
  const p1 = log.close()
  const p2 = log.close()
  t.ok(p1 === p2, 'p1 = p2')
}

test('test double close', (t) => testDoubleClose(t, new Encoder()))
