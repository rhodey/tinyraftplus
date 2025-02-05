const { xxhash64, xxhash128 } = require('hash-wasm')

class Encoder {
  constructor() {
    this.metaLen = 24
    this.bodyLen = 0
  }

  allocMeta(count = 1) {
    return Buffer.allocUnsafe(24 * count)
  }

  async encodeMeta(buf, o, seq, off, len) {
    buf.writeBigInt64BE(seq, o)
    buf.writeBigInt64BE(off, o + 8)
    buf.writeBigInt64BE(len, o + 16)
  }

  async decodeMeta(buf, o) {
    const seq = buf.readBigInt64BE(o)
    const off = buf.readBigInt64BE(o + 8)
    const len = buf.readBigInt64BE(o + 16)
    return { seq, off, len }
  }

  async encodeBody(buf) {
    return buf
  }

  async decodeBody(buf) {
    return buf
  }
}

const writeHash64 = (str, buf, off) => {
  str = Buffer.from(str, 'hex')
  buf.writeBigInt64BE(str.readBigInt64BE(0), off)
}

const writeHash128 = (str, buf, off) => {
  str = Buffer.from(str, 'hex')
  buf.writeBigInt64BE(str.readBigInt64BE(0), off)
  buf.writeBigInt64BE(str.readBigInt64BE(8), off + 8)
}

// https://xxhash.com
class XxHashEncoder {
  constructor() {
    this.metaLen = 32
    this.bodyLen = 0
  }

  allocMeta(count = 1) {
    return Buffer.allocUnsafe(32 * count)
  }

  async encodeMeta(buf, o, seq, off, len) {
    buf.writeBigInt64BE(seq, o + 8)
    buf.writeBigInt64BE(off, o + 16)
    buf.writeBigInt64BE(len, o + 24)
    const data = buf.slice(o + 8, o + 32)
    const str = await xxhash64(data)
    writeHash64(str, buf, o)
  }

  async decodeMeta(buf, o) {
    const hash = buf.slice(o, 8)
    let data = buf.slice(o + 8, o + 32)
    data = await xxhash64(data)
    data = Buffer.from(data, 'hex')
    if (!hash.equals(data)) { throw new Error('meta hash error') }
    const seq = buf.readBigInt64BE(o + 8)
    const off = buf.readBigInt64BE(o + 16)
    const len = buf.readBigInt64BE(o + 24)
    return { seq, off, len }
  }

  async encodeBody(buf) {
    return buf
  }

  async decodeBody(buf) {
    return buf
  }
}

module.exports = { Encoder, XxHashEncoder }
