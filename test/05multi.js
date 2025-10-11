const test = require('tape')
const { FsLog } = require('../src/index.js')
const { MultiFsLog } = require('../src/index.js')
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

const logFnFn = (encoder) => {
  const opts = { encoder }
  return async (multi, id) => {
    const name = `${multi.name}-m${id}`
    return new FsLog(multi.dir, name, opts)
  }
}

async function testAppendOpenCloseNew(t, encoder) {
  t.plan(41)
  t.teardown(() => log.close())

  const logFn = logFnFn(encoder)
  const opts = { encoder, logFn, maxLogLen: 64 }
  let log = new MultiFsLog('/tmp/', 'test', opts)

  await log.del()
  await log.open()

  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')

  const extra = encoder.bodyLen

  // open, close same
  let data = Buffer.from(new Array(32 - extra).fill('a').join(''))
  let seq = await log.append(data)
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.ok(data.equals(log.head), 'head = data')
  t.equal(log.logs.length, 1, 'logs = 1')
  t.equal(log.logs[0].seq, 0n, 'log seq ok')
  let llen = log.logs[0].offset + log.logs[0].hlen
  t.equal(llen, 32n, 'log length ok')

  data = Buffer.from(new Array(40 - extra).fill('b').join(''))
  seq = await log.append(data)
  t.equal(seq, 1n, 'seq = 1')
  t.equal(log.seq, 1n, 'seq = 1')
  t.ok(data.equals(log.head), 'head = data')
  t.equal(log.logs.length, 2, 'logs = 2')
  t.equal(log.logs[1].seq, 0n, 'log seq ok')

  data = Buffer.from(new Array(24 - extra).fill('c').join(''))
  seq = await log.append(data)
  t.equal(seq, 2n, 'seq = 2')
  t.equal(log.seq, 2n, 'seq = 2')
  t.ok(data.equals(log.head), 'head = data')
  t.equal(log.logs.length, 2, 'logs = 2')
  t.equal(log.logs[1].seq, 1n, 'log seq ok')
  llen = log.logs[1].offset + log.logs[1].hlen
  t.equal(llen, 40n + 24n, 'log length ok')
  await log.close()

  // open, close same
  await log.open()
  t.equal(log.seq, 2n, 'seq = 2 again')
  t.ok(data.equals(log.head), 'head = data again')
  t.equal(log.logs.length, 2, 'logs = 2')
  t.equal(log.logs[0].seq, 0n, 'log seq ok')
  t.equal(log.logs[1].seq, 1n, 'log seq ok')
  llen = log.logs[1].offset + log.logs[1].hlen
  t.equal(llen, 40n + 24n, 'log length ok')

  data = Buffer.from(new Array(16 - extra).fill('d').join(''))
  seq = await log.append(data)
  t.equal(seq, 3n, 'seq = 3')
  t.equal(log.seq, 3n, 'seq = 3')
  t.ok(data.equals(log.head), 'head = data')
  t.equal(log.logs.length, 3, 'logs = 3')
  t.equal(log.logs[2].seq, 0n, 'log seq ok')
  llen = log.logs[2].offset + log.logs[2].hlen
  t.equal(llen, 16n, 'log length ok')
  await log.close()

  // new
  log = new MultiFsLog('/tmp/', 'test', opts)
  await log.open()
  t.equal(log.seq, 3n, 'seq = 3 again')
  t.ok(data.equals(log.head), 'head = data again')
  t.equal(log.logs.length, 3, 'logs = 3')
  t.equal(log.logs[2].seq, 0n, 'log seq ok')
  llen = log.logs[2].offset + log.logs[2].hlen
  t.equal(llen, 16n, 'log length ok')

  data = { ee: 5 }
  seq = await log.append(toBuf(data))
  t.equal(seq, 4n, 'seq = 4')
  t.equal(log.seq, 4n, 'seq = 4')
  t.deepEqual(toObj(log.head), data, 'head = data')
  t.equal(log.logs.length, 3, 'logs = 3')
  t.equal(log.logs[2].seq, 1n, 'log seq ok')
}

test('test append, open, close, new', (t) => testAppendOpenCloseNew(t, new Encoder()))
test('test append, open, close, new - xxhash body', (t) => testAppendOpenCloseNew(t, new XxHashEncoder()))
test('test append, open, close, new - xxhash no body', (t) => testAppendOpenCloseNew(t, new XxHashEncoder(false)))

async function testAppendOneCloseOpen(t, encoder) {
  t.plan(10)
  t.teardown(() => log.close())

  const logFn = logFnFn(encoder)
  const opts = { encoder, logFn, maxLogLen: 64 }
  const log = new MultiFsLog('/tmp/', 'test', opts)

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

  const logFn = logFnFn(encoder)
  const opts = { encoder, logFn, rollbackCb, maxLogLen: 64 }
  const log = new MultiFsLog('/tmp/', 'test', opts)

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

  const logFn = logFnFn(encoder)
  const opts = { encoder, logFn, rollbackCb, maxLogLen: 64 }
  const log = new MultiFsLog('/tmp/', 'test', opts)

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

  const logFn = logFnFn(encoder)
  const opts = { encoder, logFn, rollbackCb, maxLogLen: 64 }
  const log = new MultiFsLog('/tmp/', 'test', opts)

  await log.del()
  await log.open()

  let data = Buffer.from(new Array(48).fill('a').join(''))
  let seq = await log.append(data)
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.ok(data.equals(log.head), 'head = data')

  data = Buffer.from(new Array(48).fill('b').join(''))
  seq = await log.append(data)
  t.equal(seq, 1n, 'seq = 1')
  t.equal(log.seq, 1n, 'seq = 1')
  t.ok(data.equals(log.head), 'head = data')

  try {
    const no = Buffer.from(new Array(48).fill('c').join(''))
    await log.append(no)
    t.fail('no error thrown')
  } catch (err) {
    t.ok(err.message.includes('test roll'), 'error thrown')
  }

  t.equal(log.seq, 1n, 'seq = 1 again')
  t.ok(data.equals(log.head), 'head = data again')
}

test('test rollback third', (t) => testRollbackThird(t, new Encoder()))
test('test rollback third - xxhash body', (t) => testRollbackThird(t, new XxHashEncoder()))
test('test rollback third - xxhash no body', (t) => testRollbackThird(t, new XxHashEncoder(false)))

async function testTrim(t, encoder) {
  t.plan(33)
  t.teardown(() => log.close())

  const logFn = logFnFn(encoder)
  const opts = { encoder, logFn, maxLogLen: 64 }
  const log = new MultiFsLog('/tmp/', 'test', opts)

  await log.del()
  await log.open()

  await log.trim(-1n)
  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')
  t.equal(log.logs.length, 0, 'logs = 0')

  let data = []
  data.push(Buffer.from(new Array(32).fill('a').join('')))
  await log.append(data[0])
  t.equal(log.logs.length, 1, 'logs = 1')

  data.push(Buffer.from(new Array(48).fill('b').join('')))
  await log.append(data[1])
  t.equal(log.logs.length, 2, 'logs = 2')

  data.push(Buffer.from(new Array(16).fill('c').join('')))
  await log.append(data[2])
  t.equal(log.logs.length, 2, 'logs = 2')

  await log.trim(-1n)
  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')
  t.equal(log.logs.length, 0, 'logs = 0')

  data = []
  data.push(Buffer.from(new Array(32).fill('d').join('')))
  await log.append(data[0])
  t.equal(log.logs.length, 1, 'logs = 1')

  data.push(Buffer.from(new Array(48).fill('e').join('')))
  await log.append(data[1])
  t.equal(log.logs.length, 2, 'logs = 2')

  data.push(Buffer.from(new Array(16).fill('f').join('')))
  await log.append(data[2])
  t.equal(log.logs.length, 2, 'logs = 2')

  data.push(Buffer.from(new Array(24).fill('f').join('')))
  await log.append(data[2])
  t.equal(log.logs.length, 3, 'logs = 3')

  await log.trim(0n)
  t.equal(log.seq, 0n, 'seq = 0')
  t.ok(data[0].equals(log.head), 'head = data again')
  t.equal(log.logs.length, 1, 'logs = 1')
  t.equal(log.logs[0].seq, 0n, 'seq = 0 again')

  data = []
  data.push(Buffer.from(new Array(32).fill('a').join('')))
  await log.append(data[0])
  t.equal(log.seq, 1n, 'seq = 1')
  t.ok(data[0].equals(log.head), 'head = data')
  t.equal(log.logs.length, 1, 'logs = 1')
  t.equal(log.logs[0].seq, 1n, 'seq = 1 again')

  data.push(Buffer.from(new Array(48).fill('b').join('')))
  await log.append(data[1])
  t.equal(log.seq, 2n, 'seq = 2')
  t.ok(data[1].equals(log.head), 'head = data')
  t.equal(log.logs.length, 2, 'logs = 2')
  t.equal(log.logs[0].seq, 1n, 'seq = 1 again')
  t.equal(log.logs[1].seq, 0n, 'seq = 0')

  data.push(Buffer.from(new Array(16).fill('c').join('')))
  await log.append(data[2])
  t.equal(log.seq, 3n, 'seq = 3')
  t.ok(data[2].equals(log.head), 'head = data')
  t.equal(log.logs.length, 2, 'logs = 2')

  await log.trim(1n)
  t.equal(log.seq, 1n, 'seq = 1')
  t.ok(data[0].equals(log.head), 'head = data')
  t.equal(log.logs.length, 1, 'logs = 1')
  t.equal(log.logs[0].seq, 1n, 'seq = 1 again')
}

test('test trim', (t) => testTrim(t, new Encoder()))

async function testTrim2(t, encoder, maxLogLen) {
  t.plan(12)
  t.teardown(() => log.close())

  const logFn = logFnFn(encoder)
  const opts = { encoder, logFn, maxLogLen }
  const log = new MultiFsLog('/tmp/', 'test', opts)

  await log.del()
  await log.open()

  let data = Buffer.from(new Array(10).fill('a').join(''))
  for (let i = 0; i < 25; i++) { await log.append(data) }
  t.equal(log.seq, 24n, 'seq = 24')
  t.ok(data.equals(log.head), 'head = data')

  let seq = 24
  data = Buffer.from(new Array(11).fill('b').join(''))
  const find1 = Buffer.from(new Array(15).fill('e').join(''))
  for (let i = 0; i < 25; i++) {
    if (++seq === 44) {
      await log.append(find1)
    } else {
      await log.append(data)
    }
  }
  t.equal(log.seq, 49n, 'seq = 49')
  t.ok(data.equals(log.head), 'head = data')

  data = Buffer.from(new Array(12).fill('c').join(''))
  const find2 = Buffer.from(new Array(16).fill('f').join(''))
  for (let i = 0; i < 25; i++) {
    if (++seq === 55) {
      await log.append(find2)
    } else {
      await log.append(data)
    }
  }
  t.equal(log.seq, 74n, 'seq = 74')
  t.ok(data.equals(log.head), 'head = data')

  data = Buffer.from(new Array(14).fill('d').join(''))
  for (let i = 0; i < 25; i++) { await log.append(data) }
  t.equal(log.seq, 99n, 'seq = 99')
  t.ok(data.equals(log.head), 'head = data')

  await log.trim(55n)
  t.equal(log.seq, 55n, 'seq = 55')
  t.ok(find2.equals(log.head), 'head = data')

  await log.trim(44n)
  t.equal(log.seq, 44n, 'seq = 44')
  t.ok(find1.equals(log.head), 'head = data')
}

test('test trim maxLogLen = 20', (t) => testTrim2(t, new Encoder(), 20))
test('test trim maxLogLen = 24', (t) => testTrim2(t, new Encoder(), 24))
test('test trim maxLogLen = 32', (t) => testTrim2(t, new Encoder(), 32))
test('test trim maxLogLen = 64', (t) => testTrim2(t, new Encoder(), 64))
test('test trim maxLogLen = 128', (t) => testTrim2(t, new Encoder(), 128))
test('test trim maxLogLen = 256', (t) => testTrim2(t, new Encoder(), 256))

async function testTrim3(t, encoder) {
  t.plan(3)
  t.teardown(() => log.close())

  const rollForwardCb = (seq) => {
    if (seq === -1n) { throw new Error('test roll') }
  }

  const logFn = logFnFn(encoder)
  const opts = { encoder, logFn, rollForwardCb, maxLogLen: 64 }
  const log = new MultiFsLog('/tmp/', 'test', opts)

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

  const logFn = logFnFn(encoder)
  const opts = { encoder, logFn, maxLogLen: 64 }
  const log = new MultiFsLog('/tmp/', 'test', opts)

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

  const logFn = logFnFn(encoder)
  const opts = { encoder, logFn, maxLogLen: 64 }
  let log = new MultiFsLog('/tmp/', 'test', opts)

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

  log = new MultiFsLog('/tmp/', 'test', opts)
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

  const logFn = logFnFn(encoder)
  const opts = { encoder, logFn, rollbackCb, maxLogLen: 64 }
  const log = new MultiFsLog('/tmp/', 'test', opts)

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

  const logFn = logFnFn(encoder)
  const opts = { encoder, logFn, rollbackCb, maxLogLen: 64 }
  const log = new MultiFsLog('/tmp/', 'test', opts)

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

  const logFn = logFnFn(encoder)
  const opts = { encoder, logFn, rollbackCb, maxLogLen: 64 }
  const log = new MultiFsLog('/tmp/', 'test', opts)

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

  const logFn = logFnFn(encoder)
  const opts = { encoder, logFn, maxLogLen: 64 }
  const log = new MultiFsLog('/tmp/', 'test', opts)

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

  const logFn = logFnFn(encoder)
  const opts = { encoder, logFn, maxLogLen: 64 }
  const log = new MultiFsLog('/tmp/', 'test', opts)

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

  const logFn = logFnFn(encoder)
  const opts = { encoder, logFn, maxLogLen: 64 }
  const log = new MultiFsLog('/tmp/', 'test', opts)

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
  t.plan(49)
  t.teardown(() => log.close())

  const logFn = logFnFn(encoder)
  const opts = { encoder, logFn, maxLogLen: 64 }
  let log = new MultiFsLog('/tmp/', 'test', opts)

  await log.del()
  await log.open()

  const extra = encoder.bodyLen

  // open, close same
  let data = Buffer.from(new Array(32 - extra).fill('a').join(''))
  let seq = await log.append(data)
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.ok(data.equals(log.head), 'head = data')
  t.equal(log.logs.length, 1, 'logs = 1')
  t.equal(log.logs[0].seq, 0n, 'log seq ok')
  let llen = log.logs[0].offset + log.logs[0].hlen
  t.equal(llen, 32n, 'log length ok')

  data = Buffer.alloc(0)
  seq = await log.append(data)
  t.equal(seq, 1n, 'seq = 1')
  t.equal(log.seq, 1n, 'seq = 1')
  t.ok(data.equals(log.head), 'head = data')
  t.equal(log.logs.length, 1, 'logs = 1')
  t.equal(log.logs[0].seq, 1n, 'log seq ok')
  llen = log.logs[0].offset + log.logs[0].hlen
  t.equal(llen, 32n + BigInt(extra), 'log length ok')

  data = Buffer.from(new Array(48 - extra).fill('b').join(''))
  seq = await log.append(data)
  t.equal(seq, 2n, 'seq = 2')
  t.equal(log.seq, 2n, 'seq = 2')
  t.ok(data.equals(log.head), 'head = data')
  t.equal(log.logs.length, 2, 'logs = 2')
  t.equal(log.logs[1].seq, 0n, 'log seq ok')
  llen = log.logs[1].offset + log.logs[1].hlen
  t.equal(llen, 48n, 'log length ok')
  await log.close()

  // open, close same
  await log.open()
  t.equal(log.seq, 2n, 'seq = 2 again')
  t.ok(data.equals(log.head), 'head = data again')
  t.equal(log.logs.length, 2, 'logs = 2')
  t.equal(log.logs[0].seq, 1n, 'log seq ok')
  t.equal(log.logs[1].seq, 0n, 'log seq ok')
  llen = log.logs[1].offset + log.logs[1].hlen
  t.equal(llen, 48n, 'log length ok')

  data = Buffer.alloc(0)
  seq = await log.append(data)
  t.equal(seq, 3n, 'seq = 3')
  t.equal(log.seq, 3n, 'seq = 3')
  t.ok(data.equals(log.head), 'head = data')
  t.equal(log.logs.length, 2, 'logs = 2')
  t.equal(log.logs[1].seq, 1n, 'log seq ok')
  llen = log.logs[1].offset + log.logs[1].hlen
  t.equal(llen, 48n + BigInt(extra), 'log length ok')
  await log.close()

  // new
  log = new MultiFsLog('/tmp/', 'test', opts)
  await log.open()
  t.equal(log.seq, 3n, 'seq = 3 again')
  t.ok(data.equals(log.head), 'head = data again')
  t.equal(log.logs.length, 2, 'logs = 2')
  t.equal(log.logs[1].seq, 1n, 'log seq ok')
  llen = log.logs[1].offset + log.logs[1].hlen
  t.equal(llen, 48n + BigInt(extra), 'log length ok')


  let logsLen = 2
  let logsSeq = 2n
  data = toBuf({ ee: 5 })
  if ((Number(llen) + data.byteLength + extra) > opts.maxLogLen) {
    logsLen = 3
    logsSeq = 0n
  }

  seq = await log.append(data)
  t.equal(seq, 4n, 'seq = 4')
  t.equal(log.seq, 4n, 'seq = 4')
  t.ok(log.head.equals(data), 'head = data')
  t.equal(log.logs.length, logsLen, 'logs = ' + logsLen)
  t.equal(log.logs[log.logs.length - 1].seq, logsSeq, 'log seq ok')

  await log.close()
  await log.del()
  await log.open()

  data = Buffer.alloc(0)
  seq = await log.append(data)
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.ok(data.equals(log.head), 'head = data')
  t.equal(log.logs.length, 1, 'logs = 1')
  t.equal(log.logs[0].seq, 0n, 'log seq ok')

  await log.close()
  await log.open()

  t.equal(log.seq, 0n, 'seq = 0')
  t.ok(data.equals(log.head), 'head = data')
  t.equal(log.logs.length, 1, 'logs = 1')
  t.equal(log.logs[0].seq, 0n, 'log seq ok')
}

test('test append buf empty', (t) => testAppendBufEmpty(t, new Encoder()))
test('test append buf empty - xxhash body', (t) => testAppendBufEmpty(t, new XxHashEncoder()))
test('test append buf empty - xxhash no body', (t) => testAppendBufEmpty(t, new XxHashEncoder(false)))

async function testBatchBufEmpty(t, encoder) {
  t.plan(16)
  t.teardown(() => log.close())

  const logFn = logFnFn(encoder)
  const opts = { encoder, logFn, maxLogLen: 64 }
  const log = new MultiFsLog('/tmp/', 'test', opts)

  await log.del()
  await log.open()

  let data = { a: 1 }
  let seq = await log.append(toBuf(data))
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = []
  data.push(Buffer.alloc(0))
  data.push(Buffer.alloc(0))
  seq = await log.appendBatch(data)
  t.equal(seq, 1n, 'seq = 1')
  t.equal(log.seq, 2n, 'seq = 2')
  t.ok(data[1].equals(log.head), 'head = data')

  await log.close()
  await log.open()

  t.equal(log.seq, 2n, 'seq = 2')
  t.ok(data[1].equals(log.head), 'head = data')

  seq = await log.appendBatch(data)
  t.equal(seq, 3n, 'seq = 1')
  t.equal(log.seq, 4n, 'seq = 2')
  t.ok(data[1].equals(log.head), 'head = data')

  await log.close()
  await log.del()
  await log.open()

  seq = await log.appendBatch(data)
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 1n, 'seq = 1')
  t.ok(data[1].equals(log.head), 'head = data')

  await log.close()
  await log.open()

  t.equal(log.seq, 1n, 'seq = 1')
  t.ok(data[1].equals(log.head), 'head = data')
}

test('test append batch buf empty', (t) => testBatchBufEmpty(t, new Encoder()))
test('test append batch buf empty - xxhash body', (t) => testBatchBufEmpty(t, new XxHashEncoder()))
test('test append batch buf empty - xxhash no body', (t) => testBatchBufEmpty(t, new XxHashEncoder(false)))

async function testDoubleOpen(t, encoder) {
  t.plan(1)
  t.teardown(() => log.close())
  const logFn = logFnFn(encoder)
  const opts = { encoder, logFn, maxLogLen: 64 }
  const log = new MultiFsLog('/tmp/', 'test', opts)
  await log.del()
  const p1 = log.open()
  const p2 = log.open()
  await p2
  t.ok(p1 === p2, 'p1 = p2')
}

test('test double open', (t) => testDoubleOpen(t, new Encoder()))

async function testDoubleClose(t, encoder) {
  t.plan(1)
  t.teardown(() => log.close())
  const logFn = logFnFn(encoder)
  const opts = { encoder, logFn, maxLogLen: 64 }
  const log = new MultiFsLog('/tmp/', 'test', opts)
  await log.del()
  await log.open()
  const p1 = log.close()
  const p2 = log.close()
  t.ok(p1 === p2, 'p1 = p2')
}

test('test double close', (t) => testDoubleClose(t, new Encoder()))
