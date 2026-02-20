const test = require('tape')
const { FsLog } = require('../src/index.js')
const { TimeoutLog } = require('../src/index.js')
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

async function testAppendThreeThenIter(t, encoder, time=false) {
  t.plan(3 + 2)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog(DIR, 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log

  await log.del()
  await log.open()

  const data = [{ a: 1 }, { bb: 2 }, { ccc: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0n
  for await (let next of log.iter(count)) {
    next = toObj(next)
    t.deepEqual(next, data[count], 'data ok')
    count++
  }

  t.pass('no errors')
  t.equal(count, 3n, 'read three bufs')
}

test('test append three then iter 0', (t) => testAppendThreeThenIter(t, new Encoder()))
test('test append three then iter 0 - time', (t) => testAppendThreeThenIter(t, new Encoder(), true))
test('test append three then iter 0 - xxhash body', (t) => testAppendThreeThenIter(t, new XxHashEncoder()))
test('test append three then iter 0 - xxhash body - time', (t) => testAppendThreeThenIter(t, new XxHashEncoder(), true))
test('test append three then iter 0 - xxhash no body', (t) => testAppendThreeThenIter(t, new XxHashEncoder(false)))
test('test append three then iter 0 - xxhash no body - time', (t) => testAppendThreeThenIter(t, new XxHashEncoder(false), true))

async function testAppendThreeThenIter2(t, encoder, time=false) {
  t.plan(2 + 2)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog(DIR, 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log

  await log.del()
  await log.open()

  const data = [{ a: 1 }, { bb: 2 }, { ccc: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 1n
  for await (let next of log.iter(count)) {
    next = toObj(next)
    t.deepEqual(next, data[count], 'data ok')
    count++
  }

  t.pass('no errors')
  t.equal(count, 3n, 'count 3')
}

test('test append three then iter 1', (t) => testAppendThreeThenIter2(t, new Encoder()))
test('test append three then iter 1 - time', (t) => testAppendThreeThenIter2(t, new Encoder(), true))
test('test append three then iter 1 - xxhash body', (t) => testAppendThreeThenIter2(t, new XxHashEncoder()))
test('test append three then iter 1 - xxhash body - time', (t) => testAppendThreeThenIter2(t, new XxHashEncoder(), true))
test('test append three then iter 1 - xxhash no body', (t) => testAppendThreeThenIter2(t, new XxHashEncoder(false)))
test('test append three then iter 1 - xxhash no body - time', (t) => testAppendThreeThenIter2(t, new XxHashEncoder(false), true))

async function testAppendOneThenIter(t, encoder, time=false) {
  t.plan(1 + 2)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog(DIR, 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log

  await log.del()
  await log.open()

  const data = [{ a: 1 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0n
  for await (let next of log.iter(count)) {
    next = toObj(next)
    t.deepEqual(next, data[count], 'data ok')
    count++
  }

  t.pass('no errors')
  t.equal(count, 1n, 'read 1 buf')
}

test('test append one then iter', (t) => testAppendOneThenIter(t, new Encoder()))
test('test append one then iter - time', (t) => testAppendOneThenIter(t, new Encoder(), true))
test('test append one then iter - xxhash body', (t) => testAppendOneThenIter(t, new XxHashEncoder()))
test('test append one then iter - xxhash body - time', (t) => testAppendOneThenIter(t, new XxHashEncoder(), true))
test('test append one then iter - xxhash no body', (t) => testAppendOneThenIter(t, new XxHashEncoder(false)))
test('test append one then iter - xxhash no body - time', (t) => testAppendOneThenIter(t, new XxHashEncoder(false), true))

async function testStepSize1(t, encoder, time=false) {
  t.plan(3 + 2)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog(DIR, 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log

  await log.del()
  await log.open()

  const data = [{ aa: 1 }, { bbb: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0n
  const optss = { iterStepSize: 1 }
  for await (let next of log.iter(count, optss)) {
    next = toObj(next)
    t.deepEqual(next, data[count], 'data ok')
    count++
  }

  t.pass('no errors')
  t.equal(count, 3n, 'read three bufs')
}

test('test append three then iter step size 1', (t) => testStepSize1(t, new Encoder()))
test('test append three then iter step size 1 - time', (t) => testStepSize1(t, new Encoder(), true))
test('test append three then iter step size 1 - xxhash body', (t) => testStepSize1(t, new XxHashEncoder()))
test('test append three then iter step size 1 - xxhash body - time', (t) => testStepSize1(t, new XxHashEncoder(), true))
test('test append three then iter step size 1 - xxhash no body', (t) => testStepSize1(t, new XxHashEncoder(false)))
test('test append three then iter step size 1 - xxhash no body - time', (t) => testStepSize1(t, new XxHashEncoder(false), true))

async function testStepSize2(t, encoder, time=false) {
  t.plan(3 + 2)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog(DIR, 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log

  await log.del()
  await log.open()

  const data = [{ a: 1 }, { bb: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0n
  const optss = { iterStepSize: 2 }
  for await (let next of log.iter(count, optss)) {
    next = toObj(next)
    t.deepEqual(next, data[count], 'data ok')
    count++
  }

  t.pass('no errors')
  t.equal(count, 3n, 'read three bufs')
}

test('test append three then iter step size 2', (t) => testStepSize2(t, new Encoder()))
test('test append three then iter step size 2 - time', (t) => testStepSize2(t, new Encoder(), true))
test('test append three then iter step size 2 - xxhash body', (t) => testStepSize2(t, new XxHashEncoder()))
test('test append three then iter step size 2 - xxhash body - time', (t) => testStepSize2(t, new XxHashEncoder(), true))
test('test append three then iter step size 2 - xxhash no body', (t) => testStepSize2(t, new XxHashEncoder(false)))
test('test append three then iter step size 2 - xxhash no body - time', (t) => testStepSize2(t, new XxHashEncoder(false), true))

async function testStepSize3(t, encoder, time=false) {
  t.plan(3 + 2)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog(DIR, 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log

  await log.del()
  await log.open()

  const data = [{ aaaa: 1 }, { bb: 2 }, { cc: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0n
  const optss = { iterStepSize: 3 }
  for await (let next of log.iter(count, optss)) {
    next = toObj(next)
    t.deepEqual(next, data[count], 'data ok')
    count++
  }

  t.pass('no errors')
  t.equal(count, 3n, 'read three bufs')
}

test('test append three then iter step size 3', (t) => testStepSize3(t, new Encoder()))
test('test append three then iter step size 3 - time', (t) => testStepSize3(t, new Encoder(), true))
test('test append three then iter step size 3 - xxhash body', (t) => testStepSize3(t, new XxHashEncoder()))
test('test append three then iter step size 3 - xxhash body - time', (t) => testStepSize3(t, new XxHashEncoder(), true))
test('test append three then iter step size 3 - xxhash no body', (t) => testStepSize3(t, new XxHashEncoder(false)))
test('test append three then iter step size 3 - xxhash no body - time', (t) => testStepSize3(t, new XxHashEncoder(false), true))

async function testStepSize4(t, encoder, time=false) {
  t.plan(3 + 2)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog(DIR, 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log

  await log.del()
  await log.open()

  const data = [{ a: 1 }, { bb: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0n
  const optss = { iterStepSize: 4 }
  for await (let next of log.iter(count, optss)) {
    next = toObj(next)
    t.deepEqual(next, data[count], 'data ok')
    count++
  }

  t.pass('no errors')
  t.equal(count, 3n, 'read three bufs')
}

test('test append three then iter step size 4', (t) => testStepSize4(t, new Encoder()))
test('test append three then iter step size 4 - time', (t) => testStepSize4(t, new Encoder(), true))
test('test append three then iter step size 4 - xxhash body', (t) => testStepSize4(t, new XxHashEncoder()))
test('test append three then iter step size 4 - xxhash body - time', (t) => testStepSize4(t, new XxHashEncoder(), true))
test('test append three then iter step size 4 - xxhash no body', (t) => testStepSize4(t, new XxHashEncoder(false)))
test('test append three then iter step size 4 - xxhash no body - time', (t) => testStepSize4(t, new XxHashEncoder(false), true))

async function testIterWithClose(t, encoder, time=false) {
  t.plan(3)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog(DIR, 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log

  await log.del()
  await log.open()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0n
  const o = { iterStepSize: 1 }
  for await (let next of log.iter(count, o)) {
    next = toObj(next)
    t.deepEqual(next, data[count], 'data ok')
    count++
    if (count === 1n) { await log.close() }
  }

  t.pass('no errors')
  t.equal(count, 1n, 'read 1 buf')
}

test('test append three then iter with close', (t) => testIterWithClose(t, new Encoder()))
test('test append three then iter with close - time', (t) => testIterWithClose(t, new Encoder(), true))
test('test append three then iter with close - xxhash body', (t) => testIterWithClose(t, new XxHashEncoder()))
test('test append three then iter with close - xxhash body - time', (t) => testIterWithClose(t, new XxHashEncoder(), true))
test('test append three then iter with close - xxhash no body', (t) => testIterWithClose(t, new XxHashEncoder(false)))
test('test append three then iter with close - xxhash no body - time', (t) => testIterWithClose(t, new XxHashEncoder(false), true))

async function testIterEnd(t, encoder, time=false) {
  t.plan(5)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog(DIR, 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log

  await log.del()
  await log.open()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0n
  const iter = log.iter(count)
  await log.append(toBuf({ d: 4 }))
  for await (let next of iter) {
    next = toObj(next)
    t.deepEqual(next, data[count], 'data ok')
    count++
  }

  t.pass('no errors')
  t.equal(count, 3n, 'read three bufs')
}

test('test iter ends based off seq at time of create', (t) => testIterEnd(t, new Encoder()))
test('test iter ends based off seq at time of create - time', (t) => testIterEnd(t, new Encoder(), true))
test('test iter ends based off seq at time of create - xxhash body', (t) => testIterEnd(t, new XxHashEncoder()))
test('test iter ends based off seq at time of create - xxhash body - time', (t) => testIterEnd(t, new XxHashEncoder(), true))
test('test iter ends based off seq at time of create - xxhash no body', (t) => testIterEnd(t, new XxHashEncoder(false)))
test('test iter ends based off seq at time of create - xxhash no body - time', (t) => testIterEnd(t, new XxHashEncoder(false), true))

async function testIterEnd2(t, encoder, time=false) {
  t.plan(1)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog(DIR, 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log

  await log.del()
  await log.open()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let seq = 3n
  const iter = log.iter(seq)
  for await (let next of iter) { seq++ }

  t.equal(seq, 3n, 'read no bufs')
}

test('test iter returns nothing if seq > log.seq', (t) => testIterEnd2(t, new Encoder()))
test('test iter returns nothing if seq > log.seq - time', (t) => testIterEnd2(t, new Encoder(), true))
test('test iter returns nothing if seq > log.seq - xxhash body', (t) => testIterEnd2(t, new XxHashEncoder()))
test('test iter returns nothing if seq > log.seq - xxhash body - time', (t) => testIterEnd2(t, new XxHashEncoder(), true))
test('test iter returns nothing if seq > log.seq - xxhash no body', (t) => testIterEnd2(t, new XxHashEncoder(false)))
test('test iter returns nothing if seq > log.seq - xxhash no body - time', (t) => testIterEnd2(t, new XxHashEncoder(false), true))

async function testIterCloseSelf(t, encoder, time=false) {
  t.plan(3)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog(DIR, 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log

  await log.del()
  await log.open()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0n
  let iter = log.iter(count)
  for await (let next of iter) {
    next = toObj(next)
    t.deepEqual(next, data[count], 'data ok')
    break
  }

  iter = log.iterators[0]
  const ok = typeof iter._open === 'boolean' && iter._open === false

  t.ok(ok, 'end loop close iter')
  t.pass('no errors')
}

test('test iter close self', (t) => testIterCloseSelf(t, new Encoder()))
test('test iter close self - time', (t) => testIterCloseSelf(t, new Encoder(), true))
test('test iter close self - xxhash body', (t) => testIterCloseSelf(t, new XxHashEncoder()))
test('test iter close self - xxhash body - time', (t) => testIterCloseSelf(t, new XxHashEncoder(), true))
test('test iter close self - xxhash no body', (t) => testIterCloseSelf(t, new XxHashEncoder(false)))
test('test iter close self - xxhash no body - time', (t) => testIterCloseSelf(t, new XxHashEncoder(false), true))

async function testIterWithThrow(t, encoder, time=false) {
  t.plan(3)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog(DIR, 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log

  await log.del()
  await log.open()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0n
  const iter = log.iter(count, {clazz: true})
  if (time) {
    iter._clazz.log._readLogs = () => Promise.reject(new Error('throws'))
  } else {
    iter.log._readLogs = () => Promise.reject(new Error('throws'))
  }

  try {
    for await (let next of iter.lazy()) { count++ }
  } catch (err) {
    t.ok(err.message.includes('throws'), 'error thrown')
  }

  const ok = typeof iter._open === 'boolean' && iter._open === false

  t.ok(ok, 'error close iter')
  t.equal(count, 0n, 'read no bufs')
}

test('test iter with throw', (t) => testIterWithThrow(t, new Encoder()))
test('test iter with throw - time', (t) => testIterWithThrow(t, new Encoder(), true))
test('test iter with throw - xxhash body', (t) => testIterWithThrow(t, new XxHashEncoder()))
test('test iter with throw - xxhash body - time', (t) => testIterWithThrow(t, new XxHashEncoder(), true))
test('test iter with throw - xxhash no body', (t) => testIterWithThrow(t, new XxHashEncoder(false)))
test('test iter with throw - xxhash no body - time', (t) => testIterWithThrow(t, new XxHashEncoder(false), true))

async function testIterWithClose2(t, encoder, time=false) {
  t.plan(2)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog(DIR, 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log

  await log.del()
  await log.open()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let seq = 0n
  const iter = log.iter(seq)
  await log.trim(-1n)

  for await (let next of iter) { seq++ }

  t.equal(seq, 0n, 'read no bufs')
  t.equal(log.iterators.length, 0, 'iter removed')
}

test('test iter closed if last > trim', (t) => testIterWithClose2(t, new Encoder()))
test('test iter closed if last > trim - time', (t) => testIterWithClose2(t, new Encoder(), true))
test('test iter closed if last > trim - xxhash body', (t) => testIterWithClose2(t, new XxHashEncoder()))
test('test iter closed if last > trim - xxhash body - time', (t) => testIterWithClose2(t, new XxHashEncoder(), true))
test('test iter closed if last > trim - xxhash no body', (t) => testIterWithClose2(t, new XxHashEncoder(false)))
test('test iter closed if last > trim - xxhash no body - time', (t) => testIterWithClose2(t, new XxHashEncoder(false), true))

async function testIterWithClose3(t, encoder, time=false) {
  t.plan(2)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog(DIR, 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log

  await log.del()
  await log.open()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let seq = 0n
  const iter = log.iter(seq)
  await log.trim(0n)

  for await (let next of iter) { seq++ }

  t.equal(seq, 0n, 'read no bufs')
  t.equal(log.iterators.length, 0, 'iter removed')
}

test('test iter closed if last > trim again', (t) => testIterWithClose3(t, new Encoder()))
test('test iter closed if last > trim again - time', (t) => testIterWithClose3(t, new Encoder(), true))
test('test iter closed if last > trim again - xxhash body', (t) => testIterWithClose3(t, new XxHashEncoder()))
test('test iter closed if last > trim again - xxhash body - time', (t) => testIterWithClose3(t, new XxHashEncoder(), true))
test('test iter closed if last > trim again - xxhash no body', (t) => testIterWithClose3(t, new XxHashEncoder(false)))
test('test iter closed if last > trim again - xxhash no body - time', (t) => testIterWithClose3(t, new XxHashEncoder(false), true))

async function testIterNotClosed(t, encoder, time=false) {
  t.plan(2)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog(DIR, 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log

  await log.del()
  await log.open()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0n
  const iter = log.iter(count)

  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  await log.trim(3n)
  for await (let next of iter) { count++ }

  t.equal(count, 3n, 'read 3 bufs')
  t.equal(log.iterators.length, 1, 'iter not removed')
}

test('test iter not closed if last < trim', (t) => testIterNotClosed(t, new Encoder()))
test('test iter not closed if last < trim - time', (t) => testIterNotClosed(t, new Encoder(), true))
test('test iter not closed if last < trim - xxhash body', (t) => testIterNotClosed(t, new XxHashEncoder()))
test('test iter not closed if last < trim - xxhash body - time', (t) => testIterNotClosed(t, new XxHashEncoder(), true))
test('test iter not closed if last < trim - xxhash no body', (t) => testIterNotClosed(t, new XxHashEncoder(false)))
test('test iter not closed if last < trim - xxhash no body - time', (t) => testIterNotClosed(t, new XxHashEncoder(false), true))

async function testIterNotClosed2(t, encoder, time=false) {
  t.plan(2)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog(DIR, 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log
  await log.del()
  await log.open()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0n
  const iter = log.iter(count)

  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  await log.trim(2n)
  for await (let next of iter) { count++ }

  t.equal(count, 3n, 'read 3 bufs')
  t.equal(log.iterators.length, 1, 'iter not removed')
}

test('test iter not closed if last = trim', (t) => testIterNotClosed2(t, new Encoder()))
test('test iter not closed if last = trim - time', (t) => testIterNotClosed2(t, new Encoder(), true))
test('test iter not closed if last = trim - xxhash body', (t) => testIterNotClosed2(t, new XxHashEncoder()))
test('test iter not closed if last = trim - xxhash body - time', (t) => testIterNotClosed2(t, new XxHashEncoder(), true))
test('test iter not closed if last = trim - xxhash no body', (t) => testIterNotClosed2(t, new XxHashEncoder(false)))
test('test iter not closed if last = trim - xxhash no body - time', (t) => testIterNotClosed2(t, new XxHashEncoder(false), true))

async function testBatchWithIter(t, encoder, time=false) {
  t.plan(5 + 2)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog(DIR, 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log
  await log.del()
  await log.open()

  const data = [{ a: 1 }, { bb: 2 }, { ccc: 3 }, { dd: 4 }, { e: 5 }]
  for (let i = 0; i < 3; i++) {
    await log.append(toBuf(data[i]))
  }

  await log.appendBatch(data.slice(3).map(toBuf))

  let count = 0n
  for await (let next of log.iter(count)) {
    next = toObj(next)
    t.deepEqual(next, data[count], 'data ok')
    count++
  }

  t.pass('no errors')
  t.equal(count, 5n, 'read 5 bufs')
}

test('test append then append batch then iter', (t) => testBatchWithIter(t, new Encoder()))
test('test append then append batch then iter - time', (t) => testBatchWithIter(t, new Encoder(), true))
test('test append then append batch then iter - xxhash body', (t) => testBatchWithIter(t, new XxHashEncoder()))
test('test append then append batch then iter - xxhash body - time', (t) => testBatchWithIter(t, new XxHashEncoder(), true))
test('test append then append batch then iter - xxhash no body', (t) => testBatchWithIter(t, new XxHashEncoder(false)))
test('test append then append batch then iter - xxhash no body - time', (t) => testBatchWithIter(t, new XxHashEncoder(false), true))

async function testAppendEmptyWithIter(t, encoder, time=false) {
  t.plan(5 + 1 + 3 + 1)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog(DIR, 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log

  await log.del()
  await log.open()

  let data = [{ a: 1 }, { bb: 2 }]
  data = data.map(toBuf)
  data.push(Buffer.alloc(0))
  data.push(Buffer.alloc(0))
  data.push(toBuf({ ccc: 3 }))
  for (const buf of data) {
    await log.append(buf)
  }

  let count = 0n
  for await (let next of log.iter(count)) {
    t.ok(data[count].equals(next), 'data ok')
    count++
  }
  t.equal(count, 5n, 'read 5 bufs')

  await log.trim(-1n)

  data = []
  data.push(Buffer.alloc(0))
  data.push(Buffer.alloc(0))
  data.push(toBuf({ ccc: 3 }))
  for (const buf of data) {
    await log.append(buf)
  }

  count = 0n
  for await (let next of log.iter(count)) {
    t.ok(data[count].equals(next), 'data ok')
    count++
  }

  t.equal(count, 3n, 'read 3 bufs')
}

test('test append empty iter', (t) => testAppendEmptyWithIter(t, new Encoder()))
test('test append empty iter - time', (t) => testAppendEmptyWithIter(t, new Encoder(), true))
test('test append empty iter - xxhash body', (t) => testAppendEmptyWithIter(t, new XxHashEncoder()))
test('test append empty iter - xxhash body - time', (t) => testAppendEmptyWithIter(t, new XxHashEncoder(), true))
test('test append empty iter- xxhash no body', (t) => testAppendEmptyWithIter(t, new XxHashEncoder(false)))
test('test append empty iter- xxhash no body - time', (t) => testAppendEmptyWithIter(t, new XxHashEncoder(false), true))

async function testAppendBatchEmptyWithIter(t, encoder, time=false) {
  t.plan(5 + 1 + 3 + 1)
  t.teardown(() => log.close())

  const opts = { encoder }
  let log = new FsLog(DIR, 'test', opts)
  log = time ? new TimeoutLog(log, {}) : log
  await log.del()
  await log.open()

  let data = [{ a: 1 }, { bb: 2 }]
  data = data.map(toBuf)
  data.push(Buffer.alloc(0))
  data.push(Buffer.alloc(0))
  data.push(toBuf({ ccc: 3 }))

  await log.appendBatch(data)

  let count = 0n
  for await (let next of log.iter(count)) {
    t.ok(data[count].equals(next), 'data ok')
    count++
  }
  t.equal(count, 5n, 'read 5 bufs')

  await log.trim(-1n)

  data = []
  data.push(Buffer.alloc(0))
  data.push(Buffer.alloc(0))
  data.push(toBuf({ ccc: 3 }))

  await log.appendBatch(data)

  count = 0n
  for await (let next of log.iter(count)) {
    t.ok(data[count].equals(next), 'data ok')
    count++
  }

  t.equal(count, 3n, 'read 3 bufs')
}

test('test append batch empty iter', (t) => testAppendBatchEmptyWithIter(t, new Encoder()))
test('test append batch empty iter - time', (t) => testAppendBatchEmptyWithIter(t, new Encoder(), true))
test('test append batch empty iter - xxhash body', (t) => testAppendBatchEmptyWithIter(t, new XxHashEncoder()))
test('test append batch empty iter - xxhash body - time', (t) => testAppendBatchEmptyWithIter(t, new XxHashEncoder(), true))
test('test append batch empty iter - xxhash no body', (t) => testAppendBatchEmptyWithIter(t, new XxHashEncoder(false)))
test('test append batch empty iter - xxhash no body - time', (t) => testAppendBatchEmptyWithIter(t, new XxHashEncoder(false), true))
