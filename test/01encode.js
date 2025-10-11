const test = require('tape')
const sodium = require('libsodium-wrappers')
const { Encoder, XxHashEncoder } = require('../src/index.js')
const { EncryptingEncoder } = require('../src/index.js')

test('test basic encoder', async (t) => {
  t.plan(6)
  const log = { path: 'test' }
  const enc = new Encoder()

  let buf = Buffer.allocUnsafe(enc.lockLen)
  await enc.encodeLock(log, buf, 0, 1n)
  let data = await enc.decodeLock(log, buf, 0)
  t.equal(data, 1n, 'seq = 1')

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

  buf = Buffer.alloc(0)
  data = await enc.encodeBody(log, buf)
  data = await enc.decodeBody(log, data)
  t.ok(buf.equals(data), 'empty body ok')
})

test('test xxhash encoder', async (t) => {
  t.plan(6)
  const log = { path: 'test' }
  const enc = new XxHashEncoder()

  let buf = Buffer.allocUnsafe(enc.lockLen)
  await enc.encodeLock(log, buf, 0, 2n)
  let data = await enc.decodeLock(log, buf, 0)
  t.equal(data, 2n, 'seq = 2')

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

  buf = Buffer.alloc(0)
  data = await enc.encodeBody(log, buf)
  data = await enc.decodeBody(log, data)
  t.ok(buf.equals(data), 'empty body ok')
})

test('test xxhash encoder no body', async (t) => {
  t.plan(6)
  const log = { path: 'test' }
  const enc = new XxHashEncoder(false)

  let buf = Buffer.allocUnsafe(enc.lockLen)
  await enc.encodeLock(log, buf, 0, 3n)
  let data = await enc.decodeLock(log, buf, 0)
  t.equal(data, 3n, 'seq = 3')

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

  buf = Buffer.alloc(0)
  data = await enc.encodeBody(log, buf)
  data = await enc.decodeBody(log, data)
  t.ok(buf.equals(data), 'empty body ok')
})

test('test xxhash encoder errors', async (t) => {
  t.plan(3)
  const log = { path: 'test' }
  const enc = new XxHashEncoder()

  let buf = Buffer.allocUnsafe(enc.lockLen)
  await enc.encodeLock(log, buf, 0, 4n)

  try {
    buf.writeUInt8(0xff, 0)
    await enc.decodeLock(log, buf, 0)
    t.fail('no lock error thrown')
  } catch (err) {
    t.ok(err.message.includes('lock corrupt'), 'lock error thrown')
  }

  buf = Buffer.allocUnsafe(enc.metaLen)
  await enc.encodeMeta(log, buf, 0, 1n, 2n, 3n)

  try {
    buf.writeUInt8(0xff, 0)
    await enc.decodeMeta(log, buf, 0)
    t.fail('no meta error thrown')
  } catch (err) {
    t.ok(err.message.includes('meta corrupt'), 'meta error thrown')
  }

  buf = Buffer.from('abc123', 'utf8')
  buf = await enc.encodeBody(log, buf)

  try {
    buf.writeUInt8(0xff, 16)
    await enc.decodeBody(log, buf)
    t.fail('no body error thrown')
  } catch (err) {
    t.ok(err.message.includes('body corrupt'), 'body error thrown')
  }
})

test('test encrypting encoder', async (t) => {
  t.plan(6)
  await sodium.ready
  const log = { path: 'test' }
  const key = sodium.crypto_generichash(32, sodium.from_string('test'))
  const enc = new EncryptingEncoder(sodium, key)

  let buf = Buffer.allocUnsafe(enc.lockLen)
  await enc.encodeLock(log, buf, 0, 2n)
  let data = await enc.decodeLock(log, buf, 0)
  t.equal(data, 2n, 'seq = 2')

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

  buf = Buffer.alloc(0)
  data = await enc.encodeBody(log, buf)
  data = await enc.decodeBody(log, data)
  t.ok(buf.equals(data), 'empty body ok')
})

test('test encrypting encoder errors', async (t) => {
  t.plan(1)
  await sodium.ready
  const log = { path: 'test' }
  const key = sodium.crypto_generichash(32, sodium.from_string('test'))
  const enc = new EncryptingEncoder(sodium, key)

  let buf = Buffer.from('test', 'utf8')
  buf = await enc.encodeBody(log, buf)

  try {
    buf.writeUInt8(0xff, 0)
    buf.writeUInt8(0xff, 1)
    buf.writeUInt8(0xff, 2)
    await enc.decodeBody(log, buf)
    t.fail('no lock error thrown')
  } catch (err) {
    t.ok(err.message.includes('decrypt error'), 'lock error thrown')
  }
})
