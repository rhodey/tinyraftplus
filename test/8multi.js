const test = require('tape')
const { MultiFsLog } = require('../lib/multi.js')
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

async function testAppendStartStopNew(t, encoder) {
  t.plan(26)
  const opts = { encoder, maxLogLen: 128 }
  let log = new MultiFsLog('/tmp/', 'test', opts)
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
  log = new MultiFsLog('/tmp/', 'test', opts)
  await log.start()
  t.equal(log.seq, '3', 'seq = 3 again')
  t.deepEqual(toObj(log.head), data, 'head = data again')

  data = { ee: 5 }
  ok = await log.append(toBuf(data))
  t.equal(ok.seq, '4', 'seq = 4')
  t.equal(log.seq, '4', 'seq = 4')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')
}

test('test append, stop, start, append, new, append', (t) => testAppendStartStopNew(t, new Encoder()))
// test('test append, stop, start, append, new, append - xxhash body', (t) => testAppendStartStopNew(t, new XxHashEncoder()))
// test('test append, stop, start, append, new, append - xxhash no body', (t) => testAppendStartStopNew(t, new XxHashEncoder(false)))
