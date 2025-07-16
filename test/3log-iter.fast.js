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

async function testAppendThreeThenIter(t, encoder) {
  t.plan(3 + 2)
  t.teardown(() => log.close())

  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.open()

  const data = [{ a: 1 }, { bb: 2 }, { ccc: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0n
  for await (let next of log.iter(count)) {
    next = toObj(next)
    t.deepEqual(next, data[count], 'data correct')
    count++
  }

  t.pass('no errors')
  t.equal(count, 3n, 'read three bufs')
}

test('test append three then iter', (t) => testAppendThreeThenIter(t, new Encoder()))
test('test append three then iter - xxhash body', (t) => testAppendThreeThenIter(t, new XxHashEncoder()))
test('test append three then iter - xxhash no body', (t) => testAppendThreeThenIter(t, new XxHashEncoder(false)))

async function testAppendOneThenIter(t, encoder) {
  t.plan(1 + 2)
  t.teardown(() => log.close())

  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.open()

  const data = [{ a: 1 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0n
  for await (let next of log.iter(count)) {
    next = toObj(next)
    t.deepEqual(next, data[count], 'data correct')
    count++
  }

  t.pass('no errors')
  t.equal(count, 1n, 'read 1 buf')
}

test('test append one then iter', (t) => testAppendOneThenIter(t, new Encoder()))
test('test append one then iter - xxhash body', (t) => testAppendOneThenIter(t, new XxHashEncoder()))
test('test append one then iter - xxhash no body', (t) => testAppendOneThenIter(t, new XxHashEncoder(false)))

async function testStepSize1(t, encoder) {
  t.plan(3 + 2)
  t.teardown(() => log.close())

  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
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
    t.deepEqual(next, data[count], 'data correct')
    count++
  }

  t.pass('no errors')
  t.equal(count, 3n, 'read three bufs')
}

test('test append three then iter step size 1', (t) => testStepSize1(t, new Encoder()))
test('test append three then iter step size 1 - xxhash body', (t) => testStepSize1(t, new XxHashEncoder()))
test('test append three then iter step size 1 - xxhash no body', (t) => testStepSize1(t, new XxHashEncoder(false)))

async function testStepSize2(t, encoder) {
  t.plan(3 + 2)
  t.teardown(() => log.close())

  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
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
    t.deepEqual(next, data[count], 'data correct')
    count++
  }

  t.pass('no errors')
  t.equal(count, 3n, 'read three bufs')
}

test('test append three then iter step size 2', (t) => testStepSize2(t, new Encoder()))
test('test append three then iter step size 2 - xxhash body', (t) => testStepSize2(t, new XxHashEncoder()))
test('test append three then iter step size 2 - xxhash no body', (t) => testStepSize2(t, new XxHashEncoder(false)))

async function testStepSize3(t, encoder) {
  t.plan(3 + 2)
  t.teardown(() => log.close())

  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
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
    t.deepEqual(next, data[count], 'data correct')
    count++
  }

  t.pass('no errors')
  t.equal(count, 3n, 'read three bufs')
}

test('test append three then iter step size 3', (t) => testStepSize3(t, new Encoder()))
test('test append three then iter step size 3 - xxhash body', (t) => testStepSize3(t, new XxHashEncoder()))
test('test append three then iter step size 3 - xxhash no body', (t) => testStepSize3(t, new XxHashEncoder(false)))

async function testStepSize4(t, encoder) {
  t.plan(3 + 2)
  t.teardown(() => log.close())

  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
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
    t.deepEqual(next, data[count], 'data correct')
    count++
  }

  t.pass('no errors')
  t.equal(count, 3n, 'read three bufs')
}

test('test append three then iter step size 4', (t) => testStepSize4(t, new Encoder()))
test('test append three then iter step size 4 - xxhash body', (t) => testStepSize4(t, new XxHashEncoder()))
test('test append three then iter step size 4 - xxhash no body', (t) => testStepSize4(t, new XxHashEncoder(false)))

async function testIterWithClose(t, encoder) {
  t.plan(4)
  t.teardown(() => log.close())

  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.open()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0n
  const optss = { iterStepSize: 1 }
  try {
    for await (let next of log.iter(count, optss)) {
      next = toObj(next)
      t.deepEqual(next, data[count], 'data correct')
      count++
      if (count === 1n) { await log.close() }
    }
  } catch (err) {
    t.ok(err.message.includes('iter is not open'), 'error thrown')
  }

  t.pass('no errors')
  t.equal(count, 1n, 'read 1 buf')
}

test('test append three then iter with close and step size 1', (t) => testIterWithClose(t, new Encoder()))
test('test append three then iter with close and step size 1 - xxhash body', (t) => testIterWithClose(t, new XxHashEncoder()))
test('test append three then iter with close and step size 1 - xxhash no body', (t) => testIterWithClose(t, new XxHashEncoder(false)))

async function testIterEnd(t, encoder) {
  t.plan(5)
  t.teardown(() => log.close())

  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
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
    t.deepEqual(next, data[count], 'data correct')
    count++
  }

  t.pass('no errors')
  t.equal(count, 3n, 'read three bufs')
}

test('test iter ends based off seq at time of create', (t) => testIterEnd(t, new Encoder()))
test('test iter ends based off seq at time of create - xxhash body', (t) => testIterEnd(t, new XxHashEncoder()))
test('test iter ends based off seq at time of create - xxhash no body', (t) => testIterEnd(t, new XxHashEncoder(false)))

async function testIterEnd2(t, encoder) {
  t.plan(1)
  t.teardown(() => log.close())

  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
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
test('test iter returns nothing if seq > log.seq - xxhash body', (t) => testIterEnd2(t, new XxHashEncoder()))
test('test iter returns nothing if seq > log.seq - xxhash no body', (t) => testIterEnd2(t, new XxHashEncoder(false)))

async function testIterWithClose2(t, encoder) {
  t.plan(3)
  t.teardown(() => log.close())

  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.open()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let seq = 0n
  const iter = log.iter(seq)
  await log.truncate(-1n)

  try {
    for await (let next of iter) { seq++ }
    t.fail('no error thrown')
  } catch (err) {
    t.ok(err.message.includes('iter is not open'), 'iter closed')
  }

  t.equal(seq, 0n, 'read no bufs')
  t.equal(log.iterators.length, 0, 'iter removed')
}

test('test iter closed if last > trunc', (t) => testIterWithClose2(t, new Encoder()))
test('test iter closed if last > trunc - xxhash body', (t) => testIterWithClose2(t, new XxHashEncoder()))
test('test iter closed if last > trunc - xxhash no body', (t) => testIterWithClose2(t, new XxHashEncoder(false)))

async function testIterWithClose3(t, encoder) {
  t.plan(3)
  t.teardown(() => log.close())

  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.open()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let seq = 0n
  const iter = log.iter(seq)
  await log.truncate(0n)

  try {
    for await (let next of iter) { seq++ }
    t.fail('no error thrown')
  } catch (err) {
    t.ok(err.message.includes('iter is not open'), 'iter closed')
  }

  t.equal(seq, 0n, 'read no bufs')
  t.equal(log.iterators.length, 0, 'iter removed')
}

test('test iter closed if last > trunc again', (t) => testIterWithClose3(t, new Encoder()))
test('test iter closed if last > trunc again - xxhash body', (t) => testIterWithClose3(t, new XxHashEncoder()))
test('test iter closed if last > trunc again - xxhash no body', (t) => testIterWithClose3(t, new XxHashEncoder(false)))

async function testIterNotClosed(t, encoder) {
  t.plan(2)
  t.teardown(() => log.close())

  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
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

  await log.truncate(3n)
  for await (let next of iter) { count++ }

  t.equal(count, 3n, 'read 3 bufs')
  t.equal(log.iterators.length, 1, 'iter not removed')
}

test('test iter not closed if last < trunc', (t) => testIterNotClosed(t, new Encoder()))
test('test iter not closed if last < trunc - xxhash body', (t) => testIterNotClosed(t, new XxHashEncoder()))
test('test iter not closed if last < trunc - xxhash no body', (t) => testIterNotClosed(t, new XxHashEncoder(false)))

async function testIterNotClosed2(t, encoder) {
  t.plan(2)
  t.teardown(() => log.close())

  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
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

  await log.truncate(2n)
  for await (let next of iter) { count++ }

  t.equal(count, 3n, 'read 3 bufs')
  t.equal(log.iterators.length, 1, 'iter not removed')
}

test('test iter not closed if last = trunc', (t) => testIterNotClosed2(t, new Encoder()))
test('test iter not closed if last = trunc - xxhash body', (t) => testIterNotClosed2(t, new XxHashEncoder()))
test('test iter not closed if last = trunc - xxhash no body', (t) => testIterNotClosed2(t, new XxHashEncoder(false)))

async function testBatchWithIter(t, encoder) {
  t.plan(5 + 2)
  t.teardown(() => log.close())

  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
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
    t.deepEqual(next, data[count], 'data correct')
    count++
  }

  t.pass('no errors')
  t.equal(count, 5n, 'read 5 bufs')
}

test('test append then append batch then iter', (t) => testBatchWithIter(t, new Encoder()))
test('test append then append batch then iter - xxhash body', (t) => testBatchWithIter(t, new XxHashEncoder()))
test('test append then append batch then iter - xxhash no body', (t) => testBatchWithIter(t, new XxHashEncoder(false)))
