const fs = require('fs')
const fsp = require('fs/promises')
const { Encoder } = require('./encoders.js')

// todo: mkdirp
// todo: BigInt in truncate
// todo: no file contents in error messages

const FS_MAX = 2 * (1024 * 1024 * 1024)
const buffer = require('node:buffer')
const BUF_MAX = buffer.kMaxLength

const noop = () => {}
const max = (a, b) => a > b ? a : b
const min = (a, b) => a < b ? a : b

const parseBigInt = (str, err) => {
  try {
    return BigInt(str)
  } catch (err2) {
    throw err
  }
}

const openOrCreate = (path, flag, again=true) => {
  return fsp.open(path, flag).then((fh) => {
    fh.name = path
    const createFlags = ['a+', 'w']
    fh.isNew = createFlags.includes(flag) // false positive ok
    return fh
  }).catch((err) => {
    if (err.code === 'ENOENT' && again) {
      return fsp.writeFile(path, Buffer.allocUnsafe(0))
        .then(() => openOrCreate(path, flag, false))
        .then((fh) => {
          fh.isNew = true
          return fh
        })
    } else if (err.code === 'ENOENT') {
      throw new Error(`${path} create error`)
    }
    throw new Error(`${path} open error - ${err.message}`)
  })
}

// fh.read does not accept pos as BigInt
const read = (fh, buf, off, len, pos) => {
  return new Promise((res, rej) => {
    fs.read(fh.fd, buf, off, len, pos, (err, bytesRead, buf) => {
      if (err) { rej(err) }
      res({ bytesRead })
    })
  })
}

const truncate = (fh, len) => fh.truncate(Number(len)).catch((err) => {throw new Error(`${fh.name} truncate error - ${err.message}`)})

const sync = async (arg) => {
  const fh = typeof arg === 'string' ? null : arg
  const path = typeof arg === 'string' ? arg : null

  if (fh) {
    return fh.sync().catch((err) => {throw new Error(`${fh.name} sync fh error - ${err.message}`)})
  }

  let fd = null
  try {
    fd = fs.openSync(path, 'r')
    fs.fsyncSync(fd)
  } catch (err) {
    throw new Error(`${path} sync fs error - ${err.message}`)
  } finally {
    if (!fd) { return }
    fs.closeSync(fd)
  }
}

const del = (path) => fsp.rm(path, {force: true}).catch((err) => {throw new Error(`${path} del error - ${err.message}`)})

const close = async (fh) => {
  if (fh === null) { return false }
  return fh.close().then(() => true).catch((err) => {
    if (err.code === 'ENOENT') { return true }
    throw new Error(`${fh.name} close error - ${err.message}`)
  })
}

const defaults = {
  iterStepSize: 1024,
  rollbackCb: noop,
  rollForwardCb: noop,
  encoder: new Encoder(),
}

class FsLog {
  constructor(dir, name, opts={}) {
    opts = { ...defaults, ...opts }
    this.rollbackCb = opts.rollbackCb
    this.rollForwardCb = opts.rollForwardCb
    this.encoder = opts.encoder
    this.dir = dir
    this.name = name
    this.path = dir + name
    this.iterators = []
    this._open = false
    this.fhlog = null
    this.fhoff = null
    this.fhlock = null
    this.offset = null
    this.hlen = null
    this.head = null
    this.seq = null
  }

  open() {
    return this._open
  }

  async _readLock(fh) {
    const { encoder: enc } = this
    fh = fh ? fh : this.fhlock
    const buf = await fh.readFile().catch((err) => {throw new Error(`${fh.name} read error - ${err.message}`)})
    if (buf.byteLength <= 0) { return null }
    if (buf.byteLength !== enc.lockLen) { throw new Error(`${fh.name} read len error`) }
    return enc.decodeLock(this, buf, 0)
  }

  async _writeLock(arg1, arg2) {
    const { encoder: enc } = this
    const fh = arg2 !== undefined ? arg1 : this.fhlock
    const seq = arg2 !== undefined ? arg2 : arg1
    if (seq === null) { return truncate(fh, 0) }
    const buf = Buffer.allocUnsafe(enc.lockLen)
    await enc.encodeLock(this, buf, 0, seq)
    const write = async () => {
      const { bytesWritten } = await fh.write(buf, 0, buf.byteLength, 0).catch((err) => {throw new Error(`${fh.name} write error - ${err.message}`)})
      if (bytesWritten !== buf.byteLength) { throw new Error(`${fh.name} write len error`) }
    }
    return write()
  }

  async _appendOffset(seq, off, len) {
    const { fhoff: fh, encoder: enc } = this
    const buf = Buffer.allocUnsafe(enc.metaLen)
    await enc.encodeMeta(this, buf, 0, seq, off, len)
    const write = async () => {
      const { bytesWritten } = await fh.write(buf).catch((err) => {throw new Error(`${fh.name} write error - ${err.message}`)})
      if (bytesWritten !== buf.byteLength) { throw new Error(`${fh.name} write len error`) }
      return BigInt(buf.byteLength)
    }
    return write()
  }

  async _appendOffsets(offs) {
    const { fhoff: fh, encoder: enc } = this
    const buf = Buffer.allocUnsafe(offs.length * enc.metaLen)
    let o = 0
    for (const off of offs) {
      await enc.encodeMeta(this, buf, o, off.seq, off.off, off.len)
      o += enc.metaLen
    }
    const write = async () => {
      const { bytesWritten } = await fh.write(buf).catch((err) => {throw new Error(`${fh.name} write error - ${err.message}`)})
      if (bytesWritten !== buf.byteLength) { throw new Error(`${fh.name} write len error`) }
      return BigInt(buf.byteLength)
    }
    return write()
  }

  async _readOffset(arg1) {
    const { encoder: enc } = this
    const fh = arg1 !== undefined ? arg1 : this.fhoff
    const { size } = await fh.stat({bigint: true}).catch((err) => {throw new Error(`${fh.name} stat error - ${err.message}`)})
    if (size <= 0n) { return null }
    const buf = Buffer.allocUnsafe(enc.metaLen)
    if (size < BigInt(buf.byteLength)) { throw new Error(`${fh.name} read size < buf len error`) }
    const pos = size - BigInt(buf.byteLength)
    const { bytesRead } = await read(fh, buf, 0, buf.byteLength, pos).catch((err) => {throw new Error(`${fh.name} read error - ${err.message}`)})
    if (bytesRead !== buf.byteLength) { throw new Error(`${fh.name} read len error`) }
    const meta = await enc.decodeMeta(this, buf, 0)
    const { seq, off, len: hlen } = meta
    return { seq, off, hlen }
  }

  async _readOffsets(seq, count) {
    const { fhoff: fh, encoder: enc } = this
    const buf = Buffer.allocUnsafe(count * enc.metaLen)
    const pos = BigInt(enc.metaLen) * seq
    const { bytesRead } = await read(fh, buf, 0, buf.byteLength, pos).catch((err) => {throw new Error(`${fh.name} read error - ${err.message}`)})
    if ((bytesRead % enc.metaLen) !== 0) { throw new Error(`${fh.name} read % error`) }
    return buf.slice(0, bytesRead)
  }

  _appendLog(data) {
    const { fhlog: fh, encoder: enc } = this
    const len = BigInt(enc.bodyLen + data.byteLength)
    const write = async () => {
      const buf = await enc.encodeBody(this, data)
      const { bytesWritten } = await fh.write(buf).catch((err) => {throw new Error(`${fh.name} write error - ${err.message}`)})
      if (bytesWritten !== Number(len)) { throw new Error(`${fh.name} write len error`) }
    }
    return { len, write }
  }

  _appendLogs(data) {
    const { fhlog: fh, encoder: enc } = this
    const lens = data.map((buf) => BigInt(enc.bodyLen + buf.byteLength))
    const write = async () => {
      const works = data.map((buf) => enc.encodeBody(this, buf))
      const bufs = await Promise.all(works)
      data = Buffer.concat(bufs)
      const { bytesWritten } = await fh.write(data).catch((err) => {throw new Error(`${fh.name} write error - ${err.message}`)})
      if (bytesWritten !== data.byteLength) { throw new Error(`${fh.name} write len error`) }
    }
    return { lens, write }
  }

  async _readHead(arg1, arg2) {
    const { encoder: enc } = this
    const fh = arg2 !== undefined ? arg1 : this.fhlog
    const offset = arg2 !== undefined ? arg2 : arg1
    const { size } = await fh.stat({bigint: true}).catch((err) => {throw new Error(`${fh.name} stat error - ${err.message}`)})
    const len = Number(size - offset)
    if (len < enc.bodyLen) { throw new Error(`${fh.name} read len < bodyLen error`) }
    const buf = Buffer.allocUnsafe(len)
    const { bytesRead } = await read(fh, buf, 0, len, offset).catch((err) => {throw new Error(`${fh.name} read error - ${err.message}`)})
    if (bytesRead !== len) { throw new Error(`${fh.name} read len error`) }
    return enc.decodeBody(this, buf)
  }

  async _readLogs(offset, len) {
    const { fhlog: fh } = this
    len = Number(len)
    const buf = Buffer.allocUnsafe(len)
    const { bytesRead } = await read(fh, buf, 0, len, offset).catch((err) => {throw new Error(`${fh.name} read error - ${err.message}`)})
    if (bytesRead !== len) { throw new Error(`${fh.name} read len error`) }
    return buf
  }

  async _handleResetLock() {
    const handles = []
    const { path, encoder: enc } = this

    try {

      const fhlock = await openOrCreate(`${path}.lock`, 'r+')
      handles.push(fhlock)
      const seq = await this._readLock(fhlock)
      if (seq === null) { return }

      const fhoff = await openOrCreate(`${path}.off`, 'r+')
      handles.push(fhoff)
      const olen = BigInt(enc.metaLen) * (seq + 1n)
      await truncate(fhoff, olen)
      const fhlog = await openOrCreate(`${path}.log`, 'r+')
      handles.push(fhlog)

      if (seq >= 0n) {
        const off = await this._readOffset(fhoff)
        const { seq: seq2, off: offset, hlen } = off
        if (seq !== seq2) { throw new Error(`${path} lock seq ${seq} and offset seq ${seq2} do not agree`) }
        const llen = offset + hlen
        await truncate(fhlog, llen)
      } else {
        await truncate(fhlog, 0n)
      }

      const works = []
      works.push(sync(fhoff))
      works.push(sync(fhlog))
      await Promise.all(works)
      await this._writeLock(fhlock, null)
        .then(() => sync(fhlock))
        .then(() => sync(this.dir))

    } finally {
      await Promise.all(handles.map(close))
    }
  }

  async start() {
    if (this._open) { return }
    const { path } = this

    const works = []
    await this._handleResetLock()
    works.push(openOrCreate(`${path}.log`, 'a+').then((fh) => this.fhlog = fh))
    works.push(openOrCreate(`${path}.off`, 'a+').then((fh) => this.fhoff = fh))
    works.push(openOrCreate(`${path}.lock`, 'r+').then((fh) => this.fhlock = fh))

    await Promise.all(works).then((arr) => {
      const neww = arr.filter((fh) => fh.isNew)
      if (neww.length <= 0) { return }
      return Promise.all(neww.map(sync)).then(() => sync(this.dir))
    })

    return this._readOffset().then((ok) => {
      if (ok === null || ok.off === -1n) {
        this.offset = -1n
        this.hlen = -1n
        this.head = null
        this.seq = -1n
        this._open = true
        return
      }
      const { seq, off, hlen } = ok
      return this._readHead(off).then((head) => {
        this.offset = off
        this.hlen = hlen
        this.head = head
        this.seq = seq
        this._open = true
      })
    })
  }

  stop() {
    if (!this._open) { return }
    const works = []
    this.iterators.forEach((iter) => iter.stop())
    this.iterators = []
    works.push(close(this.fhlog).then(() => this.fhlog = null))
    works.push(close(this.fhoff).then(() => this.fhoff = null))
    works.push(close(this.fhlock).then(() => this.fhlock = null))
    return Promise.all(works).then(() => {
      this.offset = this.hlen = this.head = this.seq = null
      this._open = false
    })
  }

  async append(data, seq=null) {
    const { path: name } = this
    const next = this.seq + 1n
    seq = seq !== null ? seq : next
    if (next !== seq) { throw new Error(`${name} next ${next} !== ${seq}`) }
    if (!(data instanceof Buffer)) { throw new Error(`${name} data must be buffer`) }
    if (!this._open) { throw new Error(`${name} is not open`) }

    // lock
    await this._writeLock(this.seq)
      .then(() => sync(this.fhlock))

    // work
    let works = []
    const { len, write } = this._appendLog(data)
    works.push(write())
    const hlen = max(this.hlen, 0n)
    const offset = max(this.offset, 0n) + hlen
    works.push(this._appendOffset(seq, offset, len))
    await Promise.all(works)

    // sync
    works = []
    works.push(sync(this.fhlog))
    works.push(sync(this.fhoff))
    await Promise.all(works)
    this.rollbackCb(seq)

    // reset
    await this._writeLock(null)
      .then(() => sync(this.fhlock))

    // ok
    this.offset = offset
    this.hlen = len
    this.head = data
    this.seq = seq
    return seq
  }

  async appendBatch(data, seq=null) {
    const { path: name } = this
    let next = this.seq + 1n
    seq = seq !== null ? seq : next
    if (next !== seq) { throw new Error(`${name} next ${next} !== ${seq}`) }
    if (!Array.isArray(data)) { throw new Error(`${name} data must be array`) }
    if (data.length <= 0) { throw new Error(`${name} data must be array with len > 0`) }
    if (!this._open) { throw new Error(`${name} is not open`) }
    const first = seq

    // lock
    await this._writeLock(this.seq)
      .then(() => sync(this.fhlock))

    // work
    let works = []
    const { lens, write } = this._appendLogs(data)
    works.push(write())

    // work
    const hlen = max(this.hlen, 0n)
    let offset = max(this.offset, 0n) + hlen
    next = lens.map((len) => {
      const next = { seq, off: offset, len }
      offset += len
      seq += 1n
      return next
    })
    works.push(this._appendOffsets(next))
    await Promise.all(works)

    // sync
    works = []
    works.push(sync(this.fhlog))
    works.push(sync(this.fhoff))
    await Promise.all(works)
    this.rollbackCb(--seq)

    // reset
    await this._writeLock(null)
      .then(() => sync(this.fhlock))

    // ok
    next = next[next.length-1]
    this.offset = next.off
    this.hlen = next.len
    this.head = data[data.length-1]
    this.seq = seq
    return first
  }

  async truncate(seq=-1n) {
    const { path: name, encoder: enc } = this
    if (typeof seq !== 'bigint') { throw new Error(`${name} seq must be big int`) }
    if (seq < -1n) { throw new Error(`${name} seq must be >= -1`) }
    if (!this._open) { throw new Error(`${name} is not open`) }
    if (this.seq <= seq) { return }

    this.iterators.filter((iter) => iter.last > seq).forEach((iter) => iter.stop())
    this.iterators = this.iterators.filter((iter) => iter._open)

    let works = []
    if (seq === -1n) {
      await this._writeLock(seq)
        .then(() => sync(this.fhlock))
      works.push(truncate(this.fhoff, 0n))
      works.push(truncate(this.fhlog, 0n))
      await Promise.all(works)
      works = []
      works.push(sync(this.fhoff))
      works.push(sync(this.fhlog))
      await Promise.all(works)
      this.rollForwardCb(-1n)
      await this._writeLock(null)
        .then(() => sync(this.fhlock))
      this.offset = -1n
      this.hlen = -1n
      this.head = null
      this.seq = -1n
      return
    }

    await this._writeLock(seq)
      .then(() => sync(this.fhlock))
    const buf = await this._readOffsets(seq, 1)
    const meta = await enc.decodeMeta(this, buf, 0)
    const { off, len: hlen } = meta
    const olen = BigInt(enc.metaLen) * (seq + 1n)
    const llen = off + hlen
    works.push(truncate(this.fhoff, olen))
    works.push(truncate(this.fhlog, llen))
    await Promise.all(works)

    works = []
    works.push(sync(this.fhlog))
    works.push(sync(this.fhoff))
    await Promise.all(works)

    this.rollForwardCb(seq)
    await this._writeLock(null)
      .then(() => sync(this.fhlock))

    return this._readHead(off).then((head) => {
      this.offset = off
      this.hlen = hlen
      this.head = head
      this.seq = seq
    })
  }

  iter(seq=0n, opts={}) {
    const { path: name } = this
    if (typeof seq !== 'bigint') { throw new Error(`${name} seq must be big int`) }
    if (seq < 0n) { throw new Error(`${name} seq must be >= 0`) }
    if (!this._open) { throw new Error(`${name} is not open`) }
    const iter = new FsIterator(this, seq, opts)
    this.iterators = this.iterators.filter((iter) => iter._open)
    this.iterators.push(iter)
    return iter.lazy()
  }

  async del() {
    const { path: name } = this
    if (this._open) { throw new Error(`${name} is open`) }
    const works = [`${this.path}.log`, `${this.path}.off`, `${this.path}.lock`]
    return Promise.all(works.map(del))
      .then(() => Promise.all(works.map(sync)))
      .then(() => sync(this.dir))
  }
}

class FsIterator {
  constructor(log, seq, opts={}) {
    opts = { name: 'iter', ...defaults, ...opts }
    this.name = opts.name
    this.path = `${log.path}-${opts.name}`
    if (opts.iterStepSize <= 0) { throw new Error(`${this.path} step size must be > 0`) }
    this.iterStepSize = opts.iterStepSize
    this._open = log.open()
    this.last = log.seq
    this.log = log
    this.seq = seq
  }

  stop() {
    this._open = false
  }

  async *lazy() {
    const { log, iterStepSize, last } = this
    const { encoder: enc } = log
    let next = this.seq

    try {
      while (next <= last) {
        const works = []
        if (!this._open) { throw new Error(`${this.path} is not open`) }
        let buf = await log._readOffsets(next, iterStepSize)
        if (!this._open) { throw new Error(`${this.path} is not open`) }

        for (let b = 0; b < buf.byteLength; b += enc.metaLen) {
          const meta = await enc.decodeMeta(this, buf, b)
          const { seq, off, len } = meta
          if (seq !== next) { throw new Error(`${this.path} seq out of order in read offsets loop`) }
          works.push([off, len])
          next = seq + 1n
          if (next > last) { break }
        }
        if (works.length <= 0) { break }

        const begin = works[0][0]
        let end = works[works.length-1]
        end = end[0] + end[1]
        buf = await log._readLogs(begin, end - begin)
        if (!this._open) { throw new Error(`${this.path} is not open`) }

        for (const work of works) {
          let [off, len] = work
          off = Number(off - begin)
          len = Number(len)
          let data = buf.slice(off, off + len)
          data = await enc.decodeBody(this, data)
          if (!this._open) { throw new Error(`${this.path} is not open`) }
          yield data
        }
      }

      if (next < (this.last + 1n)) {
        throw new Error(`${this.path} ended early - want ${this.last} got ${next}`)
      } else {
        this.stop()
      }

    } catch (err) {
      if (!err.message.includes(this.path)) {
        err.message = err.message.replace(this.log.path, this.path)
      }
      if (!err.message.includes(this.path)) {
        err.message = `${this.path} ${err.message}`
      }
      this.stop()
      throw err
    }
  }
}

module.exports = { FsLog }
