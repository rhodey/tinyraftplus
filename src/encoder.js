const { xxhash32, xxhash64, xxhash128 } = require('hash-wasm')

// no checksums
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

// lock = checksum
// meta = checksum
// body = optional
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
    const data = buf.subarray(o + 4, o + 12)
    const hash = await xxhash32(data).catch((err) => {throw new Error(`${name} lock encode make hash error`)})
    writeHash32(hash, buf, o)
  }

  async decodeLock(log, buf, o) {
    const { path: name } = log
    const hash = buf.subarray(o, o + 4)
    const data = buf.subarray(o + 4, o + 12)
    let hash2 = await xxhash32(data).catch((err) => {throw new Error(`${name} lock decode make hash error`)})
    hash2 = Buffer.from(hash2, 'hex')
    if (!hash.equals(hash2)) { throw new Error(`${name} lock corrupt`) }
    return buf.readBigInt64BE(o + 4)
  }

  async encodeMeta(log, buf, o, seq, off, len) {
    const { path: name } = log
    buf.writeBigInt64BE(seq, o + 8)
    buf.writeBigInt64BE(off, o + 16)
    buf.writeBigInt64BE(len, o + 24)
    const data = buf.subarray(o + 8, o + 32)
    const hash = await xxhash64(data).catch((err) => {throw new Error(`${name} meta encode make hash error`)})
    writeHash64(hash, buf, o)
  }

  async decodeMeta(log, buf, o) {
    const { path: name } = log
    const hash = buf.subarray(o, o + 8)
    const data = buf.subarray(o + 8, o + 32)
    let hash2 = await xxhash64(data).catch((err) => {throw new Error(`${name} meta decode make hash error`)})
    hash2 = Buffer.from(hash2, 'hex')
    if (!hash.equals(hash2)) { throw new Error(`${name} meta corrupt`) }
    const seq = buf.readBigInt64BE(o + 8)
    const off = buf.readBigInt64BE(o + 16)
    const len = buf.readBigInt64BE(o + 24)
    return { seq, off, len }
  }

  async _encodeBody(log, buf) {
    const { path: name } = log
    const hash = await xxhash128(buf).catch((err) => {throw new Error(`${name} body encode make hash error`)})
    const hashbuf = Buffer.allocUnsafe(16)
    writeHash128(hash, hashbuf, 0)
    return Buffer.concat([hashbuf, buf])
  }

  async _decodeBody(log, buf) {
    const { path: name } = log
    const hash = buf.subarray(0, 16)
    const data = buf.subarray(16)
    let hash2 = await xxhash128(data).catch((err) => {throw new Error(`${name} body decode make hash error`)})
    hash2 = Buffer.from(hash2, 'hex')
    if (!hash.equals(hash2)) { throw new Error(`${name} body corrupt`) }
    return buf.subarray(16)
  }
}

// lock = checksum
// meta = checksum
// body = encrypted (and effectively checksum)
class EncryptingEncoder {
  constructor(sodium, key) {
    this.sodium = sodium
    this.key = key
    this.base = new XxHashEncoder(false)
    this.lockLen = this.base.lockLen
    this.metaLen = this.base.metaLen
    this.bodyLen = sodium.crypto_secretbox_NONCEBYTES + sodium.crypto_secretbox_MACBYTES
  }

  encodeLock(log, buf, o, seq) {
    return this.base.encodeLock(log, buf, o, seq)
  }

  decodeLock(log, buf, o) {
    return this.base.decodeLock(log, buf, o)
  }

  encodeMeta(log, buf, o, seq, off, len) {
    return this.base.encodeMeta(log, buf, o, seq, off, len)
  }

  decodeMeta(log, buf, o) {
    return this.base.decodeMeta(log, buf, o)
  }

  nonce() {
    const { sodium } = this
    const len = sodium.crypto_secretbox_NONCEBYTES
    const nonce = sodium.randombytes_buf(len)
    return Buffer.from(nonce)
  }

  async encodeBody(log, buf) {
    const { path: name } = log
    const { sodium, key } = this

    try {

      const nonce = this.nonce()
      let ciphertext = sodium.crypto_secretbox_easy(buf, nonce, key)
      ciphertext = Buffer.from(ciphertext)
      return Buffer.concat([nonce, ciphertext])

    } catch (err) {
      throw new Error(`${name} body encrypt error - ${err.message}`)
    }
  }

  async decodeBody(log, buf) {
    const { path: name } = log
    const { sodium, key } = this
    try {

      const nonce = buf.subarray(0, sodium.crypto_secretbox_NONCEBYTES)
      const ciphertext = buf.subarray(sodium.crypto_secretbox_NONCEBYTES)
      const plaintext = sodium.crypto_secretbox_open_easy(ciphertext, nonce, key)
      return Buffer.from(plaintext)

    } catch (err) {
      throw new Error(`${name} body decrypt error - ${err.message}`)
    }
  }
}

module.exports = { Encoder, XxHashEncoder, EncryptingEncoder }
