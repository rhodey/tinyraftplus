const { xxhash64, xxhash128 } = require('hash-wasm')

class Encoder {
  constructor() {
    this.lockLen = 8
    this.metaLen = 24
    this.bodyLen = 0
  }

  async encodeLock(log, buf, o, seq) {
    buf.writeBigInt64BE(seq, o)
  }

  async decodeLock(log, buf, o) {
    return buf.readBigInt64BE(o)
  }

  async encodeMeta(log, buf, o, seq, off, len) {
    buf.writeBigInt64BE(seq, o)
    buf.writeBigInt64BE(off, o + 8)
    buf.writeBigInt64BE(len, o + 16)
  }

  async decodeMeta(log, buf, o) {
    const seq = buf.readBigInt64BE(o)
    const off = buf.readBigInt64BE(o + 8)
    const len = buf.readBigInt64BE(o + 16)
    return { seq, off, len }
  }

  async encodeBody(log, buf) {
    return buf
  }

  async decodeBody(log, buf) {
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
  constructor(body = true) {
    this.lockLen = 16
    this.metaLen = 32
    this.bodyLen = body ? 16 : basic.bodyLen
    this.encodeBody = body ? this._encodeBody : basic.encodeBody
    this.decodeBody = body ? this._decodeBody : basic.decodeBody
  }

  async encodeLock(log, buf, o, seq) {
    const { path: name } = log
    buf.writeBigInt64BE(seq, o + 8)
    const data = buf.slice(o + 8, o + 16)
    const str = await xxhash64(data).catch((err) => {throw new Error(`${name} lock encode error`)})
    writeHash64(str, buf, o)
  }

  async decodeLock(log, buf, o) {
    const { path: name } = log
    const hash = buf.slice(o, o + 8)
    let data = buf.slice(o + 8, o + 16)
    data = await xxhash64(data).catch((err) => {throw new Error(`${name} lock decode error`)})
    data = Buffer.from(data, 'hex')
    if (!hash.equals(data)) { throw new Error(`${name} lock hash incorrect`) }
    return buf.readBigInt64BE(o + 8)
  }

  async encodeMeta(log, buf, o, seq, off, len) {
    const { path: name } = log
    buf.writeBigInt64BE(seq, o + 8)
    buf.writeBigInt64BE(off, o + 16)
    buf.writeBigInt64BE(len, o + 24)
    const data = buf.slice(o + 8, o + 32)
    const str = await xxhash64(data).catch((err) => {throw new Error(`${name} meta encode error`)})
    writeHash64(str, buf, o)
  }

  async decodeMeta(log, buf, o) {
    const { path: name } = log
    const hash = buf.slice(o, o + 8)
    let data = buf.slice(o + 8, o + 32)
    data = await xxhash64(data).catch((err) => {throw new Error(`${name} meta decode error`)})
    data = Buffer.from(data, 'hex')
    if (!hash.equals(data)) { throw new Error(`${name} meta hash incorrect`) }
    const seq = buf.readBigInt64BE(o + 8)
    const off = buf.readBigInt64BE(o + 16)
    const len = buf.readBigInt64BE(o + 24)
    return { seq, off, len }
  }

  async _encodeBody(log, buf) {
    const { path: name } = log
    const str = await xxhash128(buf).catch((err) => {throw new Error(`${name} body encode error`)})
    const hash = Buffer.allocUnsafe(16)
    writeHash128(str, hash, 0)
    return Buffer.concat([hash, buf])
  }

  async _decodeBody(log, buf) {
    const { path: name } = log
    const hash = buf.slice(0, 16)
    let data = buf.slice(16)
    data = await xxhash128(data).catch((err) => {throw new Error(`${name} body decode error`)})
    data = Buffer.from(data, 'hex')
    if (!hash.equals(data)) { throw new Error(`${name} body hash incorrect`) }
    return buf.slice(16)
  }
}

module.exports = { Encoder, XxHashEncoder }
