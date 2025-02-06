const test = require('tape')
const { Encoder, XxHashEncoder } = require('../lib/encoders.js')

test('test basic encoder', async (t) => {
  t.plan(6)
  const enc = new Encoder()

  let buf = Buffer.allocUnsafe(enc.lockLen)
  await enc.encodeLock(buf, 0, 1n, 2n)
  let data = await enc.decodeLock(buf, 0)
  const { olen, llen } = data
  t.equal(olen, 1n, 'olen = 1')
  t.equal(llen, 2n, 'llen = 2')

  buf = Buffer.allocUnsafe(enc.metaLen)
  await enc.encodeMeta(buf, 0, 1n, 2n, 3n)
  data = await enc.decodeMeta(buf, 0)
  const { seq, off, len } = data
  t.equal(seq, 1n, 'seq = 1')
  t.equal(off, 2n, 'off = 2')
  t.equal(len, 3n, 'len = 3')

  buf = Buffer.allocUnsafe(enc.bodyLen)
  data = await enc.encodeBody(buf)
  data = await enc.decodeBody(data)
  t.ok(buf.equals(data), 'body ok')
})
