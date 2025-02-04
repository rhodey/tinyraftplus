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
  t.teardown(() => log.stop())

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
})

test('test append one, stop, start, append', async (t) => {
  t.plan(12)
  t.teardown(() => log.stop())

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
})

test('test rollback first', async (t) => {
  t.plan(6)
  t.teardown(() => log.stop())

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
})

test('test rollback second', async (t) => {
  t.plan(8)
  t.teardown(() => log.stop())

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
})

test('test rollback third', async (t) => {
  t.plan(12)
  t.teardown(() => log.stop())

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
})

test('test truncate, append, truncate, append, truncate, append', async (t) => {
  t.plan(12)
  t.teardown(() => log.stop())

  const log = new FsLog('/tmp/', 'test')
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
})

test('test log seq -1 and roll forward truncate -1', async (t) => {
  t.plan(4)
  t.teardown(() => log.stop())

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
})

test('test log seq 0 and roll forward truncate -1', async (t) => {
  t.plan(4)
  t.teardown(() => log.stop())

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
})

test('test append batch', async (t) => {
  t.plan(18)
  t.teardown(() => log.stop())

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

  data = [{ bb: 2 }, { ccc: 3 }]
  ok = await log.appendBatch(data.map(toBuf))
  t.equal(ok.seq, '1', 'seq = 1')
  t.equal(log.seq, '2', 'seq = 2')
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
})

test('test append batch start, stop, new', async (t) => {
  t.plan(24)
  t.teardown(() => log.stop())

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
  t.equal(ok.seq, '1', 'seq = 1')
  t.equal(log.seq, '2', 'seq = 2')
  t.deepEqual(ok.data.map(toObj), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data[1], 'head = data')
  await log.stop()

  await log.start()
  t.equal(log.seq, '2', 'seq = 2')
  t.deepEqual(toObj(log.head), data[1], 'head = data')
  data = { d: 4 }
  ok = await log.append(toBuf(data))
  t.equal(ok.seq, '3', 'seq = 3')
  t.equal(log.seq, '3', 'seq = 3')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')
  await log.stop()

  await log.start()
  t.equal(log.seq, '3', 'seq = 3')
  t.deepEqual(toObj(log.head), data, 'head = data')
  data = [{ aaa: 5 }, { bbbb: 6 }]
  ok = await log.appendBatch(data.map(toBuf))
  t.equal(ok.seq, '4', 'seq = 4')
  t.equal(log.seq, '5', 'seq = 5')
  t.deepEqual(ok.data.map(toObj), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data[1], 'head = data')
  await log.stop()

  log = new FsLog('/tmp/', 'test')
  await log.start()
  t.equal(log.seq, '5', 'seq = 5')
  t.deepEqual(toObj(log.head), data[1], 'head = data')
})

test('test rollback batch first', async (t) => {
  t.plan(6)
  t.teardown(() => log.stop())

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
})

test('test rollback batch second', async (t) => {
  t.plan(8)
  t.teardown(() => log.stop())

  const rollbackCb = (seq) => {
    if (seq === '1') { throw new Error('test roll') }
  }

  const log = new FsLog('/tmp/', 'test', { rollbackCb })
  await log.del()
  await log.start()

  const data = [{ a: 1 }]
  const ok = await log.appendBatch(data.map(toBuf))
  t.equal(ok.seq, '0', 'seq = 0')
  t.equal(log.seq, '0', 'seq = 0')
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
})

test('test rollback batch third', async (t) => {
  t.plan(12)
  t.teardown(() => log.stop())

  const rollbackCb = (seq) => {
    if (seq === '3') { throw new Error('test roll') }
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

  data = [{ bb: 2 }, { ccc: 3 }]
  ok = await log.appendBatch(data.map(toBuf))
  t.equal(ok.seq, '1', 'seq = 1')
  t.equal(log.seq, '2', 'seq = 2')
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
  t.equal(log.seq, '2', 'seq = 2 again')
  t.deepEqual(toObj(log.head), data[1], 'head = data')
})

test('test append batch then truncate -1', async (t) => {
  t.plan(2)
  t.teardown(() => log.stop())

  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  const data = [{ a: 1 }, { bb: 2 }]
  await log.appendBatch(data.map(toBuf))
  await log.truncate('-1')

  t.equal(log.seq, '-1', 'seq = -1')
  t.equal(log.head, null, 'head = null')
})

test('test append batch then truncate 0', async (t) => {
  t.plan(4)
  t.teardown(() => log.stop())

  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  let data = [{ a: 1 }, { bb: 2 }]
  await log.appendBatch(data.map(toBuf))
  await log.truncate('0')

  t.equal(log.seq, '0', 'seq = 0')
  t.deepEqual(toObj(log.head), data[0], 'head = data')

  data = [{ ccc: 3 }]
  await log.appendBatch(data.map(toBuf))
  t.equal(log.seq, '1', 'seq = 1')
  t.deepEqual(toObj(log.head), data[0], 'head = data')
})

test('test append batch then truncate 1', async (t) => {
  t.plan(6)
  t.teardown(() => log.stop())

  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  let data = [{ a: 1 }, { bb: 2 }]
  await log.appendBatch(data.map(toBuf))
  await log.truncate('0')

  t.equal(log.seq, '0', 'seq = 0')
  t.deepEqual(toObj(log.head), data[0], 'head = data')

  data = [{ ccc: 3 }, { dddd: 4 }]
  await log.appendBatch(data.map(toBuf))
  t.equal(log.seq, '2', 'seq = 2')
  t.deepEqual(toObj(log.head), data[1], 'head = data')

  await log.truncate('1')
  t.equal(log.seq, '1', 'seq = 1')
  t.deepEqual(toObj(log.head), data[0], 'head = data')
})
