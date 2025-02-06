const { xxhash64, xxhash128 } = require('hash-wasm')

class Encoder {
  constructor() {
    this.lockLen = 16
    this.metaLen = 24
    this.bodyLen = 0
  }

  name(name) { }

  async encodeLock(buf, o, olen, llen) {
    buf.writeBigInt64BE(olen, o)
    buf.writeBigInt64BE(llen, o + 8)
  }

  async decodeLock(buf, o) {
    const olen = buf.readBigInt64BE(o)
    const llen = buf.readBigInt64BE(o + 8)
    return { olen, llen }
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

const writeHash64 = (hash, buf, off) => {
  hash = Buffer.from(hash, 'hex')
  buf.writeBigInt64BE(hash.readBigInt64BE(0), off)
}

const writeHash128 = (hash, buf, off) => {
  hash = Buffer.from(hash, 'hex')
  buf.writeBigInt64BE(hash.readBigInt64BE(0), off)
  buf.writeBigInt64BE(hash.readBigInt64BE(8), off + 8)
}

const basic = new Encoder()

class XxHashEncoder {
  constructor(meta = true, body = true) {
    this.lockLen = 24
    this.metaLen = meta ? 32 : basic.metaLen
    this.bodyLen = body ? 16 : 0
    this.encodeMeta = meta ? this._encodeMeta : basic.encodeMeta
    this.decodeMeta = meta ? this._decodeMeta : basic.decodeMeta
    this.encodeBody = body ? this._encodeBody : basic.encodeBody
    this.decodeBody = body ? this._decodeBody : basic.decodeBody
    this._name = 'anon'
  }

  name(name) {
    this._name = name
  }

  async encodeLock(buf, o, olen, llen) {
    buf.writeBigInt64BE(olen, o + 8)
    buf.writeBigInt64BE(llen, o + 16)
    const data = buf.slice(o + 8, o + 24)
    const str = await xxhash64(data)
    writeHash64(str, buf, o)
  }

  async decodeLock(buf, o) {
    const { _name: name } = this
    const hash = buf.slice(o, o + 8)
    let data = buf.slice(o + 8, o + 24)
    data = await xxhash64(data)
    data = Buffer.from(data, 'hex')
    if (!hash.equals(data)) { throw new Error(`${name} lock hash error`) }
    const olen = buf.readBigInt64BE(o + 8)
    const llen = buf.readBigInt64BE(o + 16)
    return { olen, llen }
  }

  async _encodeMeta(buf, o, seq, off, len) {
    buf.writeBigInt64BE(seq, o + 8)
    buf.writeBigInt64BE(off, o + 16)
    buf.writeBigInt64BE(len, o + 24)
    const data = buf.slice(o + 8, o + 32)
    const str = await xxhash64(data)
    writeHash64(str, buf, o)
  }

  async _decodeMeta(buf, o) {
    const { _name: name } = this
    const hash = buf.slice(o, o + 8)
    let data = buf.slice(o + 8, o + 32)
    data = await xxhash64(data)
    data = Buffer.from(data, 'hex')
    if (!hash.equals(data)) { throw new Error(`${name} meta hash error`) }
    const seq = buf.readBigInt64BE(o + 8)
    const off = buf.readBigInt64BE(o + 16)
    const len = buf.readBigInt64BE(o + 24)
    return { seq, off, len }
  }

  async _encodeBody(buf) {
    const str = await xxhash128(buf)
    const hash = Buffer.allocUnsafe(16)
    writeHash128(str, hash, 0)
    return Buffer.concat([hash, buf])
  }

  async _decodeBody(buf) {
    const { _name: name } = this
    const hash = buf.slice(0, 16)
    let data = buf.slice(16)
    data = await xxhash128(data)
    data = Buffer.from(data, 'hex')
    if (!hash.equals(data)) { throw new Error(`${name} body hash error`) }
    return buf.slice(16)
  }
}

module.exports = { Encoder, XxHashEncoder }
