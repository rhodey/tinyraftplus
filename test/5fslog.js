const test = require('tape')
const { FsLog } = require('../lib/fslog.js')

test('test append 3', async (t) => {
  t.plan(14)
  const log = new FsLog('/tmp/', 'test')

  await log.start()
  t.equal(log.seq, '-1', 'seq = -1')
  t.equal(log.head, null, 'head = null')

  let data = { a: 1 }
  let ok = await log.append(data)
  t.equal(ok.seq, '0', 'seq = 0')
  t.equal(log.seq, '0', 'seq = 0')
  t.deepEqual(ok.data, data, 'ok.data = data')
  t.deepEqual(log.head, data, 'head = data')

  data = { b: 2 }
  ok = await log.append(data)
  t.equal(ok.seq, '1', 'seq = 1')
  t.equal(log.seq, '1', 'seq = 1')
  t.deepEqual(ok.data, data, 'ok.data = data')
  t.deepEqual(log.head, data, 'head = data')

  data = { c: 3 }
  ok = await log.append(data)
  t.equal(ok.seq, '2', 'seq = 2')
  t.equal(log.seq, '2', 'seq = 2')
  t.deepEqual(ok.data, data, 'ok.data = data')
  t.deepEqual(log.head, data, 'head = data')

  t.teardown(() => log.stop())
})

test('test append batch', async (t) => {
  t.plan(12)
  const log = new FsLog('/tmp/', 'test')
  await log.start()

  let data = { a: 1 }
  let ok = await log.append(data)
  t.equal(ok.seq, '0', 'seq = 0')
  t.equal(log.seq, '0', 'seq = 0')
  t.deepEqual(ok.data, data, 'ok.data = data')
  t.deepEqual(log.head, data, 'head = data')

  const arr = [{ b: 2 }, { c: 3 }]
  ok = await log.appendBatch(arr)
  t.equal(ok.seq, '1', 'seq = 1')
  t.equal(log.seq, '2', 'seq = 2')
  t.deepEqual(ok.data, arr, 'ok.data = data')
  t.deepEqual(log.head, arr[1], 'head = data')

  data = { c: 4 }
  ok = await log.append(data)
  t.equal(ok.seq, '3', 'seq = 3')
  t.equal(log.seq, '3', 'seq = 3')
  t.deepEqual(ok.data, data, 'ok.data = data')
  t.deepEqual(log.head, data, 'head = data')

  t.teardown(() => log.stop())
})

test('test append and remove', async (t) => {
  t.plan(7)
  const log = new FsLog('/tmp/', 'test')
  await log.start()

  await log.append({})
  let data = { a: 1 }
  await log.append(data)
  await log.append({})
  await log.append({})

  const removed = await log.remove('2')
  t.equal(removed, '2', 'removed = 2')
  t.equal(log.seq, '1', 'seq = 1')
  t.deepEqual(log.head, data, 'head = data')

  data = { b: 2 }
  const ok = await log.append(data)
  t.equal(ok.seq, '2', 'seq = 2')
  t.equal(log.seq, '2', 'seq = 2')
  t.deepEqual(ok.data, data, 'ok.data = data')
  t.deepEqual(log.head, data, 'head = data')

  t.teardown(() => log.stop())
})

test('test append out of order', async (t) => {
  t.plan(3)
  const log = new FsLog('/tmp/', 'test')
  await log.start()

  await log.append({})
  await log.append({})
  const ok = await log.append({}, '2')
  t.equal(ok.seq, '2', 'seq = 2')

  try {
    await log.append({}, '4')
    t.fail('error thrown')
  } catch (err) {
    t.ok(err.message.includes('!== seq'), 'error thrown')
  }

  try {
    await log.appendBatch([{ a: 1}, { b: 2 }], '4')
    t.fail('batch error thrown')
  } catch (err) {
    t.ok(err.message.includes('!== seq'), 'batch error thrown')
  }

  t.teardown(() => log.stop())
})

test('test append bad hash', async (t) => {
  t.plan(2)
  const log = new FsLog('/tmp/', 'test')
  await log.start()

  await log.append({ a: 1 })
  await log.append({ b: 2 })

  try {
    await log.append({ c: 3, prev: 'abc123' })
    t.fail('error thrown')
  } catch (err) {
    t.ok(err.message.includes('hash'), 'error thrown')
  }

  try {
    await log.appendBatch([{ c: 3, prev: 'abc123' }])
    t.fail('batch error thrown')
  } catch (err) {
    t.ok(err.message.includes('hash'), 'batch error thrown')
  }

  t.teardown(() => log.stop())
})
