const test = require('tape')
const { TinyRaftLog } = require('../index.js')

test('test start and append 3', async (t) => {
  t.plan(11)
  const log = new TinyRaftLog()

  await log.start()
  t.equal(log.seq, '-1', 'seq = -1')
  t.equal(log.head, null, 'head = null')

  let data = { a: 1 }
  let seq = await log.append(data)
  t.equal(seq, '0', 'seq = 0')
  t.equal(log.seq, '0', 'seq = 0')
  t.deepEqual(log.head, data, 'head = data')

  data = { b: 2 }
  seq = await log.append(data)
  t.equal(seq, '1', 'seq = 1')
  t.equal(log.seq, '1', 'seq = 1')
  t.deepEqual(log.head, data, 'head = data')

  data = { c: 3 }
  seq = await log.append(data)
  t.equal(seq, '2', 'seq = 2')
  t.equal(log.seq, '2', 'seq = 2')
  t.deepEqual(log.head, data, 'head = data')

  t.teardown(() => log.stop())
})

test('test append out of order', async (t) => {
  t.plan(2)
  const log = new TinyRaftLog()

  await log.start()
  await log.append({})
  await log.append({})

  const seq = await log.append({}, '2')
  t.equal(seq, '2', 'seq = 2')

  try {
    await log.append({}, '4')
    t.fail('error thrown')
  } catch (err) {
    t.ok(err, 'error thrown')
  }

  t.teardown(() => log.stop())
})

test('test append and remove', async (t) => {
  t.plan(6)
  const log = new TinyRaftLog()

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
  const seq = await log.append(data)
  t.equal(seq, '2', 'seq = 2')
  t.equal(log.seq, '2', 'seq = 2')
  t.deepEqual(log.head, data, 'head = data')

  t.teardown(() => log.stop())
})
