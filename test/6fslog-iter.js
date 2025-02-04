const test = require('tape')
const { FsLog } = require('../lib/fslog.js')

const toBuf = (obj) => {
  if (obj === null) { return null }
  obj = JSON.stringify(obj)
  return Buffer.from(obj, 'utf8')
}

const toObj = (buf) => {
  if (buf === null) { return null }
  return JSON.parse(buf.toString('utf8'))
}

test('test append three then iter', async (t) => {
  t.plan(3 + 2)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0
  for await (let next of log.iter(count.toString())) {
    next = toObj(next)
    t.deepEqual(next, data[count], 'data correct')
    count++
  }

  t.pass('no errors')
  t.equal(count, 3, 'read three bufs')
  t.teardown(() => log.stop())
})

test('test append one then iter', async (t) => {
  t.plan(1 + 2)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  const data = [{ a: 1 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0
  for await (let next of log.iter(count.toString())) {
    next = toObj(next)
    t.deepEqual(next, data[count], 'data correct')
    count++
  }

  t.pass('no errors')
  t.equal(count, 1, 'read 1 buf')
  t.teardown(() => log.stop())
})

test('test append three then iter step size 1', async (t) => {
  t.plan(3 + 2)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0
  const opts = { iterStepSize: 1 }
  for await (let next of log.iter(count.toString(), opts)) {
    next = toObj(next)
    t.deepEqual(next, data[count], 'data correct')
    count++
  }

  t.pass('no errors')
  t.equal(count, 3, 'read three bufs')
  t.teardown(() => log.stop())
})

test('test append three then iter step size 2', async (t) => {
  t.plan(3 + 2)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0
  const opts = { iterStepSize: 2 }
  for await (let next of log.iter(count.toString(), opts)) {
    next = toObj(next)
    t.deepEqual(next, data[count], 'data correct')
    count++
  }

  t.pass('no errors')
  t.equal(count, 3, 'read three bufs')
  t.teardown(() => log.stop())
})

test('test append three then iter step size 3', async (t) => {
  t.plan(3 + 2)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0
  const opts = { iterStepSize: 3 }
  for await (let next of log.iter(count.toString(), opts)) {
    next = toObj(next)
    t.deepEqual(next, data[count], 'data correct')
    count++
  }

  t.pass('no errors')
  t.equal(count, 3, 'read three bufs')
  t.teardown(() => log.stop())
})

test('test append three then iter step size 4', async (t) => {
  t.plan(3 + 2)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0
  const opts = { iterStepSize: 4 }
  for await (let next of log.iter(count.toString(), opts)) {
    next = toObj(next)
    t.deepEqual(next, data[count], 'data correct')
    count++
  }

  t.pass('no errors')
  t.equal(count, 3, 'read three bufs')
  t.teardown(() => log.stop())
})

test('test append three then iter with stop and step size 1', async (t) => {
  t.plan(4)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0
  const opts = { iterStepSize: 1 }
  try {
    for await (let next of log.iter(count.toString(), opts)) {
      next = toObj(next)
      t.deepEqual(next, data[count], 'data correct')
      count++
      if (count === 1) { await log.stop() }
    }
  } catch (err) {
    t.ok(err.message.includes('iter is not open'), 'error thrown')
  }

  t.pass('no errors')
  t.equal(count, 1, 'read 1 buf')
  t.teardown(() => log.stop())
})

test('test iter stops based off seq at time of create', async (t) => {
  t.plan(5)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0
  const iter = log.iter(count.toString())
  await log.append(toBuf({ d: 4 }))
  for await (let next of iter) {
    next = toObj(next)
    t.deepEqual(next, data[count], 'data correct')
    count++
  }

  t.pass('no errors')
  t.equal(count, 3, 'read three bufs')
  t.teardown(() => log.stop())
})

test('test iter returns nothing if seq > log.seq', async (t) => {
  t.plan(1)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let seq = 3
  const iter = log.iter(seq.toString())
  for await (let next of iter) { seq++ }

  t.equal(seq, 3, 'read no bufs')
  t.teardown(() => log.stop())
})

test('test iter stopped if last > trunc', async (t) => {
  t.plan(3)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let seq = 0
  const iter = log.iter(seq.toString())
  await log.truncate('-1')

  try {
    for await (let next of iter) { seq++ }
    t.fail('no error thrown')
  } catch (err) {
    t.ok(err.message.includes('iter is not open'), 'iter stopped')
  }

  t.equal(seq, 0, 'read no bufs')
  t.equal(log.iterators.length, 0, 'iter removed')
  t.teardown(() => log.stop())
})

test('test iter stopped if last > trunc again', async (t) => {
  t.plan(3)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let seq = 0
  const iter = log.iter(seq.toString())
  await log.truncate('0')

  try {
    for await (let next of iter) { seq++ }
    t.fail('no error thrown')
  } catch (err) {
    t.ok(err.message.includes('iter is not open'), 'iter stopped')
  }

  t.equal(seq, 0, 'read no bufs')
  t.equal(log.iterators.length, 0, 'iter removed')
  t.teardown(() => log.stop())
})

test('test iter not stopped if last < trunc', async (t) => {
  t.plan(2)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0
  const iter = log.iter(count.toString())

  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  await log.truncate('3')
  for await (let next of iter) { count++ }

  t.equal(count, 3, 'read 3 bufs')
  t.equal(log.iterators.length, 1, 'iter not removed')
  t.teardown(() => log.stop())
})

test('test iter not stopped if last = trunc', async (t) => {
  t.plan(2)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let count = 0
  const iter = log.iter(count.toString())

  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  await log.truncate('2')
  for await (let next of iter) { count++ }

  t.equal(count, 3, 'read 3 bufs')
  t.equal(log.iterators.length, 1, 'iter not removed')
  t.teardown(() => log.stop())
})

test('test append then append batch then iter', async (t) => {
  t.plan(5 + 2)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  const data = [{ a: 1 }, { bb: 2 }, { ccc: 3 }, { dd: 4 }, { e: 5 }]
  for (let i = 0; i < 3; i++) {
    await log.append(toBuf(data[i]))
  }

  await log.appendBatch(data.slice(3).map(toBuf))

  let count = 0
  for await (let next of log.iter(count.toString())) {
    next = toObj(next)
    t.deepEqual(next, data[count], 'data correct')
    count++
  }

  t.pass('no errors')
  t.equal(count, 5, 'read 5 bufs')
  t.teardown(() => log.stop())
})
