const test = require('tape')
const { FsLog } = require('../lib/fslog.js')

// todo: test start, stop, start
test('test append, stop, start, append', async (t) => {
  t.plan(20)
  let log = new FsLog('/tmp/', 'test')

  await log.del()
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
  await log.stop()

  log = new FsLog('/tmp/', 'test')
  await log.start()
  t.equal(log.seq, '2', 'seq = 2 again')
  t.deepEqual(log.head, data, 'head = data again')

  data = { d: 5 }
  ok = await log.append(data)
  t.equal(ok.seq, '3', 'seq = 3')
  t.equal(log.seq, '3', 'seq = 3')
  t.deepEqual(ok.data, data, 'ok.data = data')
  t.deepEqual(log.head, data, 'head = data')

  t.teardown(() => log.stop())
})

test('test append one, stop, start, append', async (t) => {
  t.plan(12)
  let log = new FsLog('/tmp/', 'test')

  await log.del()
  await log.start()
  t.equal(log.seq, '-1', 'seq = -1')
  t.equal(log.head, null, 'head = null')

  let data = { a: 1 }
  let ok = await log.append(data)
  t.equal(ok.seq, '0', 'seq = 0')
  t.equal(log.seq, '0', 'seq = 0')
  t.deepEqual(ok.data, data, 'ok.data = data')
  t.deepEqual(log.head, data, 'head = data')
  await log.stop()

  log = new FsLog('/tmp/', 'test')
  await log.start()
  t.equal(log.seq, '0', 'seq = 0 again')
  t.deepEqual(log.head, data, 'head = data again')

  data = { b: 2 }
  ok = await log.append(data)
  t.equal(ok.seq, '1', 'seq = 1')
  t.equal(log.seq, '1', 'seq = 1')
  t.deepEqual(ok.data, data, 'ok.data = data')
  t.deepEqual(log.head, data, 'head = data')

  t.teardown(() => log.stop())
})
