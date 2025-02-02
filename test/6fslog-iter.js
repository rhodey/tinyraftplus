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
  t.plan(5)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let seq = 0
  for await (let next of log.iter(seq.toString())) {
    next = toObj(next)
    t.deepEqual(next, data[seq], 'data correct')
    seq++
  }

  t.pass('no errors')
  t.equal(seq, 3, 'read three bufs')
  t.teardown(() => log.stop())
})

test('test append one then iter', async (t) => {
  t.plan(3)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  const data = [{ a: 1 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let seq = 0
  for await (let next of log.iter(seq.toString())) {
    next = toObj(next)
    t.deepEqual(next, data[seq], 'data correct')
    seq++
  }

  t.pass('no errors')
  t.equal(seq, 1, 'read 1 buf')
  t.teardown(() => log.stop())
})

test('test append three then iter step size 1', async (t) => {
  t.plan(5)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let seq = 0
  const opts = { iterStepSize: 1 }
  for await (let next of log.iter(seq.toString(), opts)) {
    next = toObj(next)
    t.deepEqual(next, data[seq], 'data correct')
    seq++
  }

  t.pass('no errors')
  t.equal(seq, 3, 'read three bufs')
  t.teardown(() => log.stop())
})

test('test append three then iter step size 2', async (t) => {
  t.plan(5)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let seq = 0
  const opts = { iterStepSize: 2 }
  for await (let next of log.iter(seq.toString(), opts)) {
    next = toObj(next)
    t.deepEqual(next, data[seq], 'data correct')
    seq++
  }

  t.pass('no errors')
  t.equal(seq, 3, 'read three bufs')
  t.teardown(() => log.stop())
})

test('test append three then iter step size 3', async (t) => {
  t.plan(5)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let seq = 0
  const opts = { iterStepSize: 3 }
  for await (let next of log.iter(seq.toString(), opts)) {
    next = toObj(next)
    t.deepEqual(next, data[seq], 'data correct')
    seq++
  }

  t.pass('no errors')
  t.equal(seq, 3, 'read three bufs')
  t.teardown(() => log.stop())
})

test('test append three then iter step size 4', async (t) => {
  t.plan(5)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const obj of data) {
    await log.append(toBuf(obj))
  }

  let seq = 0
  const opts = { iterStepSize: 4 }
  for await (let next of log.iter(seq.toString(), opts)) {
    next = toObj(next)
    t.deepEqual(next, data[seq], 'data correct')
    seq++
  }

  t.pass('no errors')
  t.equal(seq, 3, 'read three bufs')
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

  let seq = 0
  const opts = { iterStepSize: 1 }
  try {
    for await (let next of log.iter(seq.toString(), opts)) {
      next = toObj(next)
      t.deepEqual(next, data[seq], 'data correct')
      seq++
      if (seq === 1) { await log.stop() }
    }
  } catch (err) {
    t.ok(err.message.includes('iter is not open'), 'error thrown')
  }

  t.pass('no errors')
  t.equal(seq, 1, 'read three bufs')
  t.teardown(() => log.stop())
})
