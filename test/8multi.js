const test = require('tape')
const { FsLog } = require('../lib/fslog.js')
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

const logFnFn = (encoder) => {
  const opts = { encoder }
  return async (multi, id) => {
    const name = `${multi.name}-m${id}`
    return new FsLog(multi.dir, name, opts)
  }
}

async function testAppendStartStopNew(t, encoder) {
  t.plan(33)
  const logFn = logFnFn(encoder)
  const opts = { encoder, logFn, maxLogLen: 64 }
  let log = new MultiFsLog('/tmp/', 'test', opts)
  t.teardown(() => log.stop())

  await log.del()
  await log.start()
  t.equal(log.seq, -1n, 'seq = -1')
  t.equal(log.head, null, 'head = null')

  const extra = encoder.bodyLen

  // start, stop same
  let data = Buffer.from(new Array(32 - extra).fill('a').join(''))
  let seq = await log.append(data)
  t.equal(seq, 0n, 'seq = 0')
  t.equal(log.seq, 0n, 'seq = 0')
  t.ok(data.equals(log.head), 'head = data')
  t.equal(log.logs.length, 1, 'logs = 1')
  let llen = log.logs[0].offset + log.logs[0].hlen
  t.equal(llen, 32n, 'log length correct')

  data = Buffer.from(new Array(40 - extra).fill('b').join(''))
  seq = await log.append(data)
  t.equal(seq, 1n, 'seq = 1')
  t.equal(log.seq, 1n, 'seq = 1')
  t.ok(data.equals(log.head), 'head = data')
  t.equal(log.logs.length, 2, 'logs = 2')

  data = Buffer.from(new Array(24 - extra).fill('c').join(''))
  seq = await log.append(data)
  t.equal(seq, 2n, 'seq = 2')
  t.equal(log.seq, 2n, 'seq = 2')
  t.ok(data.equals(log.head), 'head = data')
  t.equal(log.logs.length, 2, 'logs = 2')
  llen = log.logs[1].offset + log.logs[1].hlen
  t.equal(llen, 40n + 24n, 'log length correct')
  await log.stop()

  // start, stop same
  await log.start()
  t.equal(log.seq, 2n, 'seq = 2 again')
  t.ok(data.equals(log.head), 'head = data again')
  t.equal(log.logs.length, 2, 'logs = 2')
  llen = log.logs[1].offset + log.logs[1].hlen
  t.equal(llen, 40n + 24n, 'log length correct')

  data = Buffer.from(new Array(16 - extra).fill('d').join(''))
  seq = await log.append(data)
  t.equal(seq, 3n, 'seq = 3')
  t.equal(log.seq, 3n, 'seq = 3')
  t.ok(data.equals(log.head), 'head = data')
  t.equal(log.logs.length, 3, 'logs = 3')
  llen = log.logs[2].offset + log.logs[2].hlen
  t.equal(llen, 16n, 'log length correct')
  await log.stop()

  // new
  log = new MultiFsLog('/tmp/', 'test', opts)
  await log.start()
  t.equal(log.seq, 3n, 'seq = 3 again')
  t.ok(data.equals(log.head), 'head = data again')
  t.equal(log.logs.length, 3, 'logs = 3')
  llen = log.logs[2].offset + log.logs[2].hlen
  t.equal(llen, 16n, 'log length correct')

  data = { ee: 5 }
  seq = await log.append(toBuf(data))
  t.equal(seq, 4n, 'seq = 4')
  t.equal(log.seq, 4n, 'seq = 4')
  t.deepEqual(toObj(log.head), data, 'head = data')
  t.equal(log.logs.length, 3, 'logs = 3')
}

test('test append, stop, start, append, new, append', (t) => testAppendStartStopNew(t, new Encoder()))
test('test append, stop, start, append, new, append - xxhash body', (t) => testAppendStartStopNew(t, new XxHashEncoder()))
test('test append, stop, start, append, new, append - xxhash no body', (t) => testAppendStartStopNew(t, new XxHashEncoder(false)))

async function testTruncate(t, encoder) {
  t.plan(22)
  const logFn = logFnFn(encoder)
  const opts = { encoder, logFn, maxLogLen: 64 }
  let log = new MultiFsLog('/tmp/', 'test', opts)
  t.teardown(() => log.stop())

  await log.del()
  await log.start()

  await log.truncate(-1n)
  t.equal(log.seq, -1n, 'seq = -1')
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

  await log.truncate(-1n)
  t.equal(log.seq, -1n, 'seq = -1')
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

  await log.truncate(0n)
  t.equal(log.seq, 0n, 'seq = 0')
  t.ok(data[0].equals(log.head), 'head = data again')
  t.equal(log.logs.length, 1, 'logs = 1')

  data = []
  data.push(Buffer.from(new Array(32).fill('a').join('')))
  await log.append(data[0])
  t.equal(log.seq, 1n, 'seq = 1')
  t.equal(log.logs.length, 1, 'logs = 1')

  data.push(Buffer.from(new Array(48).fill('b').join('')))
  await log.append(data[1])
  t.equal(log.logs.length, 2, 'logs = 2')

  data.push(Buffer.from(new Array(16).fill('c').join('')))
  await log.append(data[2])
  t.equal(log.logs.length, 2, 'logs = 2')

  await log.truncate(1n)
  t.equal(log.seq, 1n, 'seq = 1')
  t.ok(data[0].equals(log.head), 'head = data again')
  t.equal(log.logs.length, 1, 'logs = 1')
}

test('test truncate many', (t) => testTruncate(t, new Encoder()))

async function testTruncate2(t, encoder, maxLogLen) {
  t.plan(12)
  const logFn = logFnFn(encoder)
  const opts = { encoder, logFn, maxLogLen }
  let log = new MultiFsLog('/tmp/', 'test', opts)
  t.teardown(() => log.stop())

  await log.del()
  await log.start()

  let data = Buffer.from(new Array(10).fill('a').join(''))
  for (let i = 0; i < 25; i++) { await log.append(data) }
  t.equal(log.seq, 24n, 'seq = 24')
  t.ok(data.equals(log.head), 'head = data')

  let seq = 24
  data = Buffer.from(new Array(11).fill('b').join(''))
  const find1 = Buffer.from(new Array(15).fill('e').join(''))
  for (let i = 0; i < 25; i++) {
    if (++seq === 44) {
      await log.append(find1)
    } else {
      await log.append(data)
    }
  }
  t.equal(log.seq, 49n, 'seq = 49')
  t.ok(data.equals(log.head), 'head = data')

  data = Buffer.from(new Array(12).fill('c').join(''))
  const find2 = Buffer.from(new Array(16).fill('f').join(''))
  for (let i = 0; i < 25; i++) {
    if (++seq === 55) {
      await log.append(find2)
    } else {
      await log.append(data)
    }
  }
  t.equal(log.seq, 74n, 'seq = 74')
  t.ok(data.equals(log.head), 'head = data')

  data = Buffer.from(new Array(14).fill('d').join(''))
  for (let i = 0; i < 25; i++) { await log.append(data) }
  t.equal(log.seq, 99n, 'seq = 99')
  t.ok(data.equals(log.head), 'head = data')

  await log.truncate(55n)
  t.equal(log.seq, 55n, 'seq = 55')
  t.ok(find2.equals(log.head), 'head = data')

  await log.truncate(44n)
  t.equal(log.seq, 44n, 'seq = 44')
  t.ok(find1.equals(log.head), 'head = data')
}

test('test truncate maxLogLen = 20', (t) => testTruncate2(t, new Encoder(), 20))
test('test truncate maxLogLen = 24', (t) => testTruncate2(t, new Encoder(), 24))
test('test truncate maxLogLen = 32', (t) => testTruncate2(t, new Encoder(), 32))
test('test truncate maxLogLen = 64', (t) => testTruncate2(t, new Encoder(), 64))
test('test truncate maxLogLen = 128', (t) => testTruncate2(t, new Encoder(), 128))
test('test truncate maxLogLen = 256', (t) => testTruncate2(t, new Encoder(), 256))
