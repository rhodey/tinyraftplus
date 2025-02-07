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
  t.plan(33)
  const opts = { encoder, maxLogLen: 64 }
  let log = new MultiFsLog('/tmp/', 'test', opts)
  t.teardown(() => log.stop())

  await log.del()
  await log.start()
  t.equal(log.seq, '-1', 'seq = -1')
  t.equal(log.head, null, 'head = null')

  // start, stop same
  let data = Buffer.from(new Array(32).fill('a').join(''))
  let ok = await log.append(data)
  t.equal(ok.seq, '0', 'seq = 0')
  t.equal(log.seq, '0', 'seq = 0')
  t.ok(data.equals(ok.data), 'ok.data = data')
  t.ok(data.equals(log.head), 'head = data')
  t.equal(log.logs.length, 1, 'logs = 1')

  data = Buffer.from(new Array(48).fill('b').join(''))
  ok = await log.append(data)
  t.equal(ok.seq, '1', 'seq = 1')
  t.equal(log.seq, '1', 'seq = 1')
  t.ok(data.equals(ok.data), 'ok.data = data')
  t.ok(data.equals(log.head), 'head = data')
  t.equal(log.logs.length, 2, 'logs = 2')

  data = Buffer.from(new Array(16).fill('c').join(''))
  ok = await log.append(data)
  t.equal(ok.seq, '2', 'seq = 2')
  t.equal(log.seq, '2', 'seq = 2')
  t.ok(data.equals(ok.data), 'ok.data = data')
  t.ok(data.equals(log.head), 'head = data')
  t.equal(log.logs.length, 2, 'logs = 2')
  await log.stop()

  // start, stop same
  await log.start()
  t.equal(log.seq, '2', 'seq = 2 again')
  t.ok(data.equals(log.head), 'head = data again')
  t.equal(log.logs.length, 2, 'logs = 2')

  data = Buffer.from(new Array(16).fill('d').join(''))
  ok = await log.append(data)
  t.equal(ok.seq, '3', 'seq = 3')
  t.equal(log.seq, '3', 'seq = 3')
  t.ok(data.equals(ok.data), 'ok.data = data')
  t.ok(data.equals(log.head), 'head = data')
  t.equal(log.logs.length, 3, 'logs = 3')
  await log.stop()

  // new
  log = new MultiFsLog('/tmp/', 'test', opts)
  await log.start()
  t.equal(log.seq, '3', 'seq = 3 again')
  t.ok(data.equals(log.head), 'head = data again')
  t.equal(log.logs.length, 3, 'logs = 3')

  data = { ee: 5 }
  ok = await log.append(toBuf(data))
  t.equal(ok.seq, '4', 'seq = 4')
  t.equal(log.seq, '4', 'seq = 4')
  t.deepEqual(toObj(ok.data), data, 'ok.data = data')
  t.deepEqual(toObj(log.head), data, 'head = data')
  t.equal(log.logs.length, 3, 'logs = 3')
}

test('test append, stop, start, append, new, append', (t) => testAppendStartStopNew(t, new Encoder()))
// test('test append, stop, start, append, new, append - xxhash body', (t) => testAppendStartStopNew(t, new XxHashEncoder()))
// test('test append, stop, start, append, new, append - xxhash no body', (t) => testAppendStartStopNew(t, new XxHashEncoder(false)))

async function testTruncate(t, encoder) {
  t.plan(21)
  const opts = { encoder, maxLogLen: 64 }
  let log = new MultiFsLog('/tmp/', 'test', opts)
  t.teardown(() => log.stop())

  await log.del()
  await log.start()

  await log.truncate('-1')
  t.equal(log.seq, '-1', 'seq = -1')
  t.equal(log.head, null, 'head = null')
  t.equal(log.logs.length, 0, 'logs = 0')

  let data = []
  data.push(Buffer.from(new Array(32).fill('a').join('')))
  await log.append(data[0])
  t.equal(log.logs.length, 1, 'logs = 1')

  data.push(Buffer.from(new Array(48).fill('b').join('')))
  await log.append(data[1])
  t.equal(log.logs.length, 2, 'logs = 2')

  data.push(Buffer.from(new Array(16).fill('c').join('')))
  await log.append(data[2])
  t.equal(log.logs.length, 2, 'logs = 2')

  await log.truncate('-1')
  t.equal(log.seq, '-1', 'seq = -1')
  t.equal(log.head, null, 'head = null')
  t.equal(log.logs.length, 0, 'logs = 0')

  data = []
  data.push(Buffer.from(new Array(32).fill('d').join('')))
  await log.append(data[0])
  t.equal(log.logs.length, 1, 'logs = 1')

  data.push(Buffer.from(new Array(48).fill('e').join('')))
  await log.append(data[1])
  t.equal(log.logs.length, 2, 'logs = 2')

  data.push(Buffer.from(new Array(16).fill('f').join('')))
  await log.append(data[2])
  t.equal(log.logs.length, 2, 'logs = 2')

  await log.truncate('0')
  t.equal(log.seq, '0', 'seq = 0')
  t.ok(data[0].equals(log.head), 'head = data again')
  t.equal(log.logs.length, 1, 'logs = 1')

  data = []
  data.push(Buffer.from(new Array(32).fill('a').join('')))
  await log.append(data[0])
  t.equal(log.logs.length, 1, 'logs = 1')

  data.push(Buffer.from(new Array(48).fill('b').join('')))
  await log.append(data[1])
  t.equal(log.logs.length, 2, 'logs = 2')

  data.push(Buffer.from(new Array(16).fill('c').join('')))
  await log.append(data[2])
  t.equal(log.logs.length, 2, 'logs = 2')

  await log.truncate('1')
  t.equal(log.seq, '1', 'seq = 1')
  t.ok(data[1].equals(log.head), 'head = data again')
  t.equal(log.logs.length, 2, 'logs = 2')
}

test('test truncate', (t) => testTruncate(t, new Encoder()))
