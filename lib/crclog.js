const { FsLog } = require('./fslog.js')
const { xxhash64, xxhash128, md5 } = require('hash-wasm')

const exampleMeta = Buffer.alloc(24)

const exampleBody = Buffer.alloc(64)

const writeHash = (str, buf, off) => {
  str = Buffer.from(str, 'hex')
  buf.writeUInt32BE(str.readUInt32BE(0), off)
  buf.writeUInt32BE(str.readUInt32BE(4), off + 4)
}

class Encoder {
  constructor() {
    this.metaLen = 8 + 24
    this.bodyLen = 16
  }

  async encodeMeta(buf, o, seq, off, len) {
    buf.writeBigInt64BE(seq, o + 8)
    buf.writeBigInt64BE(off, o + 8 + 8)
    buf.writeBigInt64BE(len, o + 8 + 16)
    const data = buf.slice(o + 8, o + 8 + 24)
    const str = await xxhash64(data)
    writeHash(str, buf, o)
  }

  async decodeMeta(buf) {
    const seq = buf.readBigInt64BE(0)
    const off = buf.readBigInt64BE(8)
    const len = buf.readBigInt64BE(16)
    return { seq, off, len }
  }

  async encodeBody(buf) {
    return buf
  }

  async decodeBody(buf) {
    return buf
  }
}

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

const defaults = {
  crcMeta: xxHashMeta,
  crcBody: xxHashBody,
}

class CrcFsLog extends FsLog {
  constructor(dir, name, opts={}) {
    opts = { ...defaults, ...opts }
    super(dir, name, opts)
    this.crcMeta = opts.crcMeta
    this.crcBody = opts.crcBody
    this.metaExtra = -1
    this.bodyExtra = -1
  }

  async start() {
    await super.start()
    const { path: name } = this
    if (this.metaExtra >= 0 && this.bodyExtra >= 0) { return }
    const works = []
    works.push(this.crcMeta(exampleMeta).then((buf) => {
      this.metaExtra = buf.byteLength - exampleMeta.byteLength
    }))
    works.push(this.crcBody(exampleBody).then((buf) => {
      this.bodyExtra = buf.byteLength - exampleBody.byteLength
    }))
    await Promise.all(works)
      .catch((err) => {throw new Error(`${name} crc error - ${err.message}`)})
  }

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
