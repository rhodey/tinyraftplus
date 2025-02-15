const { xxhash32, xxhash64, xxhash128 } = require('hash-wasm')

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

const writeHash32 = (hash, buf, off) => {
  hash = Buffer.from(hash, 'hex')
  buf.writeUInt32BE(hash.readUInt32BE(0), off)
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
    this.lockLen = 12
    this.metaLen = 32
    this.bodyLen = body ? 16 : basic.bodyLen
    this.encodeBody = body ? this._encodeBody : basic.encodeBody
    this.decodeBody = body ? this._decodeBody : basic.decodeBody
  }

  async encodeLock(log, buf, o, seq) {
    const { path: name } = log
    buf.writeBigInt64BE(seq, o + 4)
    const data = buf.slice(o + 4, o + 12)
    const hash = await xxhash32(data).catch((err) => {throw new Error(`${name} lock encode error`)})
    writeHash32(hash, buf, o)
  }

  async decodeLock(log, buf, o) {
    const { path: name } = log
    const hash = buf.slice(o, o + 4)
    const data = buf.slice(o + 4, o + 12)
    let hash2 = await xxhash32(data).catch((err) => {throw new Error(`${name} lock decode error`)})
    hash2 = Buffer.from(hash2, 'hex')
    if (!hash.equals(hash2)) { throw new Error(`${name} lock hash incorrect`) }
    return buf.readBigInt64BE(o + 4)
  }

  async encodeMeta(log, buf, o, seq, off, len) {
    const { path: name } = log
    buf.writeBigInt64BE(seq, o + 8)
    buf.writeBigInt64BE(off, o + 16)
    buf.writeBigInt64BE(len, o + 24)
    const data = buf.slice(o + 8, o + 32)
    const hash = await xxhash64(data).catch((err) => {throw new Error(`${name} meta encode error`)})
    writeHash64(hash, buf, o)
  }

  async decodeMeta(log, buf, o) {
    const { path: name } = log
    const hash = buf.slice(o, o + 8)
    const data = buf.slice(o + 8, o + 32)
    let hash2 = await xxhash64(data).catch((err) => {throw new Error(`${name} meta decode error`)})
    hash2 = Buffer.from(hash2, 'hex')
    if (!hash.equals(hash2)) { throw new Error(`${name} meta hash incorrect`) }
    const seq = buf.readBigInt64BE(o + 8)
    const off = buf.readBigInt64BE(o + 16)
    const len = buf.readBigInt64BE(o + 24)
    return { seq, off, len }
  }

  async _encodeBody(log, buf) {
    const { path: name } = log
    const hash = await xxhash128(buf).catch((err) => {throw new Error(`${name} body encode error`)})
    const hashbuf = Buffer.allocUnsafe(16)
    writeHash128(hash, hashbuf, 0)
    return Buffer.concat([hashbuf, buf])
  }

  async _decodeBody(log, buf) {
    const { path: name } = log
    const hash = buf.slice(0, 16)
    const data = buf.slice(16)
    let hash2 = await xxhash128(data).catch((err) => {throw new Error(`${name} body decode error`)})
    hash2 = Buffer.from(hash2, 'hex')
    if (!hash.equals(hash2)) { throw new Error(`${name} body hash incorrect`) }
    return buf.slice(16)
  }
}

class EncryptingEncoder {
  constructor(sodium, key) {
    this.sodium = sodium
    this.key = key
  }

  async encode(log, seq, prev, body) {
    const { path: name } = log
    if (prev === null || prev === undefined) { prev = Buffer.from('null', 'utf8') }
    const meta = Buffer.allocUnsafe(24)
    meta.writeBigInt64BE(seq, 0)
    const hash = await xxhash128(prev).catch((err) => {throw new Error(`${name} body encode error`)})
    writeHash128(hash, meta, 8)
    const plaintext = Buffer.concat([meta, body])
    const { sodium, key } = this
    let nonce = sodium.randombytes_buf(sodium.crypto_secretbox_NONCEBYTES)
    let ciphertext = sodium.crypto_secretbox_easy(plaintext, nonce, key)
    nonce = Buffer.from(nonce)
    ciphertext = Buffer.from(ciphertext)
    return Buffer.concat([nonce, ciphertext])
  }

  async decode(log, buf) {
    const { path: name } = log
    if (buf === null || buf === undefined) {
      return { seq: -1n, prev: null, body: null }
    }
    const { sodium, key } = this
    const nonce = buf.slice(0, sodium.crypto_secretbox_NONCEBYTES)
    const ciphertext = buf.slice(sodium.crypto_secretbox_NONCEBYTES)
    let plaintext = sodium.crypto_secretbox_open_easy(ciphertext, nonce, key)
    plaintext = Buffer.from(plaintext)
    const seq = plaintext.readBigInt64BE(0)
    const prev = plaintext.slice(8, 24)
    const body = plaintext.slice(24)
    return { seq, prev, body }
  }
}

module.exports = { Encoder, XxHashEncoder, EncryptingEncoder }
