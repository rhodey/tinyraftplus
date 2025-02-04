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

test('test append, stop, start, append, new, append', async (t) => {
  t.plan(26)
  let log = new FsLog('/tmp/', 'test')

  await log.del()
  await log.start()
  t.equal(log.seq, '-1', 'seq = -1')
  t.equal(log.head, null, 'head = null')

  // start, stop same
  let data = { a: 1 }
  let ok = await log.append(toBuf(data))
  t.equal(ok.seq, '0', 'seq = 0')
  t.equal(log.seq, '0', 'seq = 0')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = { bb: 2 }
  ok = await log.append(toBuf(data))
  t.equal(ok.seq, '1', 'seq = 1')
  t.equal(log.seq, '1', 'seq = 1')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = { ccc: 3 }
  ok = await log.append(toBuf(data))
  t.equal(ok.seq, '2', 'seq = 2')
  t.equal(log.seq, '2', 'seq = 2')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')
  await log.stop()

  // start, stop same
  await log.start()
  t.equal(log.seq, '2', 'seq = 2 again')
  t.deepEqual(toObj(log.head), data, 'head = data again')

  data = { d: 4 }
  ok = await log.append(toBuf(data))
  t.equal(ok.seq, '3', 'seq = 3')
  t.equal(log.seq, '3', 'seq = 3')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')
  await log.stop()

  // new
  log = new FsLog('/tmp/', 'test')
  await log.start()
  t.equal(log.seq, '3', 'seq = 3 again')
  t.deepEqual(toObj(log.head), data, 'head = data again')

  data = { ee: 5 }
  ok = await log.append(toBuf(data))
  t.equal(ok.seq, '4', 'seq = 4')
  t.equal(log.seq, '4', 'seq = 4')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')

  t.teardown(() => log.stop())
})

test('test append one, stop, start, append', async (t) => {
  t.plan(12)
  const log = new FsLog('/tmp/', 'test')

  await log.del()
  await log.start()
  t.equal(log.seq, '-1', 'seq = -1')
  t.equal(log.head, null, 'head = null')

  let data = { a: 1 }
  let ok = await log.append(toBuf(data))
  t.equal(ok.seq, '0', 'seq = 0')
  t.equal(log.seq, '0', 'seq = 0')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')
  await log.stop()

  await log.start()
  t.equal(log.seq, '0', 'seq = 0 again')
  t.deepEqual(toObj(log.head), data, 'head = data again')

  data = { b: 2 }
  ok = await log.append(toBuf(data))
  t.equal(ok.seq, '1', 'seq = 1')
  t.equal(log.seq, '1', 'seq = 1')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')

  t.teardown(() => log.stop())
})

test('test rollback first', async (t) => {
  t.plan(6)

  const rollbackCb = (seq) => {
    if (seq === '0') { throw new Error('test roll') }
  }

  const log = new FsLog('/tmp/', 'test', { rollbackCb })
  await log.del()
  await log.start()

  t.equal(log.seq, '-1', 'seq = -1')
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
  t.equal(log.seq, '-1', 'seq = -1 again')
  t.equal(log.head, null, 'head = null again')

  t.teardown(() => log.stop())
})

test('test rollback second', async (t) => {
  t.plan(8)

  const rollbackCb = (seq) => {
    if (seq === '1') { throw new Error('test roll') }
  }

  const log = new FsLog('/tmp/', 'test', { rollbackCb })
  await log.del()
  await log.start()

  const data = { a: 1 }
  const ok = await log.append(toBuf(data))
  t.equal(ok.seq, '0', 'seq = 0')
  t.equal(log.seq, '0', 'seq = 0')
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
  t.equal(log.seq, '0', 'seq = 0 again')
  t.deepEqual(toObj(log.head), data, 'head = data again')

  t.teardown(() => log.stop())
})

test('test rollback third', async (t) => {
  t.plan(12)

  const rollbackCb = (seq) => {
    if (seq === '2') { throw new Error('test roll') }
  }

  const log = new FsLog('/tmp/', 'test', { rollbackCb })
  await log.del()
  await log.start()

  let data = { a: 1 }
  let ok = await log.append(toBuf(data))
  t.equal(ok.seq, '0', 'seq = 0')
  t.equal(log.seq, '0', 'seq = 0')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = { b: 2 }
  ok = await log.append(toBuf(data))
  t.equal(ok.seq, '1', 'seq = 1')
  t.equal(log.seq, '1', 'seq = 1')
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
  t.equal(log.seq, '1', 'seq = 1 again')
  t.deepEqual(toObj(log.head), data, 'head = data again')

  t.teardown(() => log.stop())
})

test('test truncate, append, truncate, append, truncate, append', async (t) => {
  t.plan(12)
  let log = new FsLog('/tmp/', 'test')

  await log.del()
  await log.start()

  await log.truncate('-1')
  t.equal(log.seq, '-1', 'seq = -1')
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

  await log.truncate('-1')
  t.equal(log.seq, '-1', 'seq = -1')
  t.equal(log.head, null, 'head = null')

  i = 2
  await log.append(data[++i])
  t.deepEqual(toObj(log.head), toObj(data[i]), 'head = data')
  await log.append(data[++i])
  t.deepEqual(toObj(log.head), toObj(data[i]), 'head = data')

  await log.truncate('0')
  t.equal(log.seq, '0', 'seq = 0')
  t.deepEqual(toObj(log.head), toObj(data[3]), 'head = data')

  i = 4
  await log.append(data[++i])
  t.deepEqual(toObj(log.head), toObj(data[i]), 'head = data')
  await log.append(data[++i])
  t.deepEqual(toObj(log.head), toObj(data[i]), 'head = data')

  t.teardown(() => log.stop())
})

test('test log seq -1 and roll forward truncate -1', async (t) => {
  t.plan(4)

  const rollForwardCb = (seq) => {
    if (seq === '-1') { throw new Error('test roll') }
  }

  const log = new FsLog('/tmp/', 'test', { rollForwardCb })
  await log.del()
  await log.start()

  await log.truncate('-1')
  t.pass('no error')

  await log.stop()
  await log.start()
  t.pass('restart ok')
  t.equal(log.seq, '-1', 'seq = -1 again')
  t.equal(log.head, null, 'head = null')

  t.teardown(() => log.stop())
})

test('test log seq 0 and roll forward truncate -1', async (t) => {
  t.plan(4)

  const rollForwardCb = (seq) => {
    if (seq === '-1') { throw new Error('test roll') }
  }

  const log = new FsLog('/tmp/', 'test', { rollForwardCb })
  await log.del()
  await log.start()

  const data = []
  for (let i = 0; i < 10; i++) {
    data.push(toBuf({ i }))
  }

  await log.append(data[0])

  try {
    await log.truncate('-1')
    t.fail('no error thrown')
  } catch (err) {
    t.ok(err.message.includes('test roll'), 'error thrown')
  }

  await log.stop()
  await log.start()
  t.pass('restart ok')
  t.equal(log.seq, '-1', 'seq = -1')
  t.deepEqual(log.head, null, 'head = data')

  t.teardown(() => log.stop())
})

test('test append batch', async (t) => {
  t.plan(19)
  let log = new FsLog('/tmp/', 'test')

  await log.del()
  await log.start()
  t.equal(log.seq, '-1', 'seq = -1')
  t.equal(log.head, null, 'head = null')

  let data = { a: 1 }
  let ok = await log.append(toBuf(data))
  t.equal(ok.seq, '0', 'seq = 0')
  t.equal(log.seq, '0', 'seq = 0')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = [{ bb: 2 }, { ccc: 3 }]
  ok = await log.appendBatch(data.map(toBuf))
  t.equal(ok.first, '1', 'first = 1')
  t.equal(ok.last, '2', 'last = 2')
  t.equal(log.seq, '2', 'log seq = 2')
  t.deepEqual(ok.data.map(toObj), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data[1], 'head = data')

  data = { d: 4 }
  ok = await log.append(toBuf(data))
  t.equal(ok.seq, '3', 'seq = 3')
  t.equal(log.seq, '3', 'seq = 3')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')

  data = { eeee: 5 }
  ok = await log.append(toBuf(data))
  t.equal(ok.seq, '4', 'seq = 4')
  t.equal(log.seq, '4', 'seq = 4')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')

  t.teardown(() => log.stop())
})

test('test rollback batch first', async (t) => {
  t.plan(6)

  const rollbackCb = (seq) => {
    if (seq === '0') { throw new Error('test roll') }
  }

  const log = new FsLog('/tmp/', 'test', { rollbackCb })
  await log.del()
  await log.start()

  t.equal(log.seq, '-1', 'seq = -1')
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
  t.equal(log.seq, '-1', 'seq = -1 again')
  t.equal(log.head, null, 'head = null again')

  t.teardown(() => log.stop())
})

test('test rollback batch second', async (t) => {
  t.plan(9)

  const rollbackCb = (seq) => {
    if (seq === '1') { throw new Error('test roll') }
  }

  const log = new FsLog('/tmp/', 'test', { rollbackCb })
  await log.del()
  await log.start()

  const data = [{ a: 1 }]
  const ok = await log.appendBatch(data.map(toBuf))
  t.equal(ok.first, '0', 'first = 0')
  t.equal(ok.last, '0', 'last = 0')
  t.equal(log.seq, '0', 'log seq = 0')
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
  t.equal(log.seq, '0', 'seq = 0 again')
  t.deepEqual(toObj(log.head), data[0], 'head = data')

  t.teardown(() => log.stop())
})
