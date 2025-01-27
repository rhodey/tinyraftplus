const test = require('tape')
const { TinyRaftLog } = require('../index.js')

test('test start and append 3', async (t) => {
  t.plan(11)
  const log = new TinyRaftLog()

  await log.start()
  t.equal(log.seq, -1, 'seq = -1')
  let head = log.head
  t.equal(head, null, 'head = null')

  let data = { a: 1 }
  let seq = await log.append(0, data)
  t.equal(seq, 0, 'seq = 0')
  t.equal(log.seq, 0, 'seq = 0')
  head = log.head
  t.deepEqual(data, head, 'head = data')

  data = { b: 2 }
  seq = await log.append(1, data)
  t.equal(seq, 1, 'seq = 1')
  t.equal(log.seq, 1, 'seq = 1')
  head = log.head
  t.deepEqual(data, head, 'head = data')

  data = { c: 3 }
  seq = await log.append(2, data)
  t.equal(seq, 2, 'seq = 2')
  t.equal(log.seq, 2, 'seq = 2')
  head = log.head
  t.deepEqual(data, head, 'head = data')

  t.teardown(() => log.stop())
})

test('test append out of order', async (t) => {
  t.plan(2)
  const log = new TinyRaftLog()

  await log.start()
  await log.append(0, {})
  await log.append(1, {})

  let seq = await log.append(2, {})
  t.equal(seq, 2, 'seq = 2')

  seq = await log.append(4, {})
  t.equal(seq, 2, 'seq = 2')

  t.teardown(() => log.stop())
})

test('test append and remove', async (t) => {
  t.plan(3)
  const log = new TinyRaftLog()

  await log.start()
  await log.append(0, {})
  const data = { a: 1}
  await log.append(1, data)
  await log.append(2, {})
  await log.append(3, {})

  const removed = await log.remove(2)
  t.equal(removed, 2, 'removed = 2')
  t.equal(log.seq, 1, 'seq = 1')

  const head = log.head
  t.deepEqual(head, data, 'head = data')

  t.teardown(() => log.stop())
})
