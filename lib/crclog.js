const { xxhash64, xxhash128 } = require('hash-wasm')

const writeHash64 = (str, buf, off) => {
  str = Buffer.from(str, 'hex')
  buf.writeBigInt64BE(str.readBigInt64BE(0), off)
}

const writeHash128 = (str, buf, off) => {
  str = Buffer.from(str, 'hex')
  buf.writeBigInt64BE(str.readBigInt64BE(0), off)
  buf.writeBigInt64BE(str.readBigInt64BE(8), off + 8)
}

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
    const hash = buf.readBigInt64BE(o)
    // todo: test hash
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

/*
const xxHashMeta = async (buf) => {
  let hash = await xxhash64(idk)
  hash = Buffer.from(hash, 'utf8')
  return Buffer.concat([hash, buf])
}

const xxHashBody = async (buf) => {
  let hash = await xxhash128(idk)
  hash = Buffer.from(hash, 'utf8')
  return Buffer.concat([hash, buf])
}

async function main() {
  const idk = Buffer.from('abc1233333')
  hash = await xxhash64(idk)
  console.log(123, hash)
  hash = Buffer.from(hash, 'hex')
  console.log(123, hash.byteLength)
  hash = await xxhash128(idk)
  console.log(456, hash)
  hash = Buffer.from(hash, 'hex')
  console.log(456, hash.byteLength)
}

main()
*/

module.exports = { XxHashEncoder }
