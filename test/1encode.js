const test = require('tape')
const { Encoder, XxHashEncoder } = require('../lib/encoders.js')

test('test basic encoder', async (t) => {
  t.plan(6)
  const log = { path: 'test' }
  const enc = new Encoder()

  let buf = Buffer.allocUnsafe(enc.lockLen)
  await enc.encodeLock(log, buf, 0, 1n, 2n)
  let data = await enc.decodeLock(log, buf, 0)
  const { olen, llen } = data
  t.equal(olen, 1n, 'olen = 1')
  t.equal(llen, 2n, 'llen = 2')

  buf = Buffer.allocUnsafe(enc.metaLen)
  await enc.encodeMeta(log, buf, 0, 1n, 2n, 3n)
  data = await enc.decodeMeta(log, buf, 0)
  const { seq, off, len } = data
  t.equal(seq, 1n, 'seq = 1')
  t.equal(off, 2n, 'off = 2')
  t.equal(len, 3n, 'len = 3')

  buf = Buffer.from('abc123', 'utf8')
  data = await enc.encodeBody(log, buf)
  data = await enc.decodeBody(log, data)
  t.ok(buf.equals(data), 'body ok')
})

test('test xxhash encoder', async (t) => {
  t.plan(6)
  const log = { path: 'test' }
  const enc = new XxHashEncoder()

  let buf = Buffer.allocUnsafe(enc.lockLen)
  await enc.encodeLock(log, buf, 0, 1n, 2n)
  let data = await enc.decodeLock(log, buf, 0)
  const { olen, llen } = data
  t.equal(olen, 1n, 'olen = 1')
  t.equal(llen, 2n, 'llen = 2')

  buf = Buffer.allocUnsafe(enc.metaLen)
  await enc.encodeMeta(log, buf, 0, 1n, 2n, 3n)
  data = await enc.decodeMeta(log, buf, 0)
  const { seq, off, len } = data
  t.equal(seq, 1n, 'seq = 1')
  t.equal(off, 2n, 'off = 2')
  t.equal(len, 3n, 'len = 3')

  buf = Buffer.from('abc123', 'utf8')
  data = await enc.encodeBody(log, buf)
  data = await enc.decodeBody(log, data)
  t.ok(buf.equals(data), 'body ok')
})

test('test xxhash encoder no body', async (t) => {
  t.plan(6)
  const log = { path: 'test' }
  const enc = new XxHashEncoder(false)

  let buf = Buffer.allocUnsafe(enc.lockLen)
  await enc.encodeLock(log, buf, 0, 1n, 2n)
  let data = await enc.decodeLock(log, buf, 0)
  const { olen, llen } = data
  t.equal(olen, 1n, 'olen = 1')
  t.equal(llen, 2n, 'llen = 2')

  buf = Buffer.allocUnsafe(enc.metaLen)
  await enc.encodeMeta(log, buf, 0, 1n, 2n, 3n)
  data = await enc.decodeMeta(log, buf, 0)
  const { seq, off, len } = data
  t.equal(seq, 1n, 'seq = 1')
  t.equal(off, 2n, 'off = 2')
  t.equal(len, 3n, 'len = 3')

  buf = Buffer.from('abc123', 'utf8')
  data = await enc.encodeBody(log, buf)
  data = await enc.decodeBody(log, data)
  t.ok(buf.equals(data), 'body ok')
})

test('test xxhash encoder errors', async (t) => {
  t.plan(3)
  const log = { path: 'test' }
  const enc = new XxHashEncoder()

  let buf = Buffer.allocUnsafe(enc.lockLen)
  await enc.encodeLock(log, buf, 0, 1n, 2n)

  try {
    buf.writeUInt8(0xff, 0)
    await enc.decodeLock(log, buf, 0)
    t.fail('no lock error thrown')
  } catch (err) {
    t.ok(err.message.includes('lock hash incorrect'), 'lock error thrown')
  }

  buf = Buffer.allocUnsafe(enc.metaLen)
  await enc.encodeMeta(log, buf, 0, 1n, 2n, 3n)

  try {
    buf.writeUInt8(0xff, 0)
    await enc.decodeMeta(log, buf, 0)
    t.fail('no meta error thrown')
  } catch (err) {
    t.ok(err.message.includes('meta hash incorrect'), 'meta error thrown')
  }

  buf = Buffer.from('abc123', 'utf8')
  buf = await enc.encodeBody(log, buf)

  try {
    buf.writeUInt8(0xff, 16)
    await enc.decodeBody(log, buf)
    t.fail('no body error thrown')
  } catch (err) {
    t.ok(err.message.includes('body hash incorrect'), 'body error thrown')
  }
})
