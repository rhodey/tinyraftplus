const fs = require('fs')
const fsp = require('fs/promises')
const { Encoder } = require('./encoders.js')
const { FsLog } = require('./fslog.js')

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
    fh.isNew = createFlags.includes(flag)
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

const listDir = (dir) => {
  return fsp.readdir(dir, {withFileTypes: true}).then((arr) => {
    return arr.filter((e) => e.isFile())
      .map((f) => f.name)
  })
}

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
    if (err.code === 'ENOENT') { return false }
    throw new Error(`${fh.name} close error - ${err.message}`)
  })
}

const logFn = (multi, id) => {
  const name = `${multi.name}-m${id}`
  return new FsLog(multi.dir, name)
}

const logsFn = (multi) => {
  const prefix = `${multi.name}-m`
  let files = await lsDir(multi.dir)
  files = files.filter((name) => name.startsWith(prefix))
  let ids = files.map((name) => name.substring(prefix.length))
  ids = ids.map((name) => parseInt(name.split('.')[0]))
  ids = ids.filter((id) => !isNaN(id)).sort()
  return ids.map((id) => multi.logFn(multi, id))
}

const defaults = {
  iterStepSize: 1024,
  maxLogLen: 1024 * 1024 * 1024,
  encoder: new Encoder(),
  logFn, logsFn,
}

class MultiFsLog {
  constructor(dir, name, opts={}) {
    opts = { ...defaults, ...opts }
    this.maxLogLen = BigInt(opts.maxLogLen)
    this.encoder = opts.encoder
    this.logFn = opts.logFn
    this.logsFn = opts.logsFn
    this.dir = dir
    this.name = name
    this.path = dir + name
    this.encoder.name(this.path)
    this.iterators = []
    this.encoder = null
    this._open = false
    this.fhlock = null
    this.logs = null
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
    return enc.decodeLock(buf, 0)
  }

  async _writeLock(arg1, arg2, arg3) {
    const { encoder: enc } = this
    const fh = arg3 !== undefined ? arg1 : this.fhlock
    const olen = arg3 !== undefined ? arg2 : arg1
    const llen = arg3 !== undefined ? arg3 : arg2
    if (olen === null || llen === null) { return truncate(fh, 0) }
    const buf = Buffer.allocUnsafe(enc.lockLen)
    await enc.encodeLock(buf, 0, olen, llen)
    const write = async () => {
      const { bytesWritten } = await fh.write(buf, 0, buf.byteLength, 0).catch((err) => {throw new Error(`${fh.name} write error - ${err.message}`)})
      if (bytesWritten !== buf.byteLength) { throw new Error(`${fh.name} write len error`) }
    }
    return write()
  }

  async start() {
    if (this._open) { return }
    const works = []
    works.push(openOrCreate(`${this.path}.lock`, 'r+').then((fh) => this.fhlock = fh))
    works.push(this.logsFn(this))
    const [_, logs] = await Promise.all(works)

    if (logs.length <= 0) {
      this.logs = null
      this.seq = '-1'
      this._open = true
      return
    }

    await Promise.all(logs.map((log) => log.start()))
    logs.forEach((log, id) => log.mid = id)
    this.logs = logs
    this.seq = '-1'
    this._open = true
  }

  stop() {
    const works = []
    this.iterators.forEach((iter) => iter.stop())
    this.iterators = []
    works.push(close(this.fhlock).then(() => this.fhlock = null))
    if (this.logs) { this.logs.forEach((log) => works.push(log.stop())) }
    return Promise.all(works).then(() => {
      this.logs = this.seq = null
      this._open = false
    })
  }

  async _firstLog() {
    const first = this.logFn(this, 0)
    first.mid = 0
    this.logs = [first]
    await first.start()
    return first
  }

  async _nextLog(data) {
    if (!this.logs) { return this._firstLog() }
    let log = this.logs[this.logs.length-1]
    const dataLen = data.byteLength + log.encoder.bodyLen
    const logLen = log.offset + log.hlen
    const nextLen = logLen + BigInt(dataLen)
    if (nextLen > this.maxLogLen) {
      log = this.logFn(this, log.mid+1)
      this.logs.push(log)
      await log.start()
    }
    return log
  }

  async append(data, seq=null) {
    const { path: name } = this
    let next = BigInt(this.seq) + 1n
    seq = seq !== null ? seq : next
    seq = parseBigInt(seq, new Error(`${name} seq must be string number`))
    if (next !== seq) { throw new Error(`${name} next ${next} !== ${seq}`) }
    if (!this._open) { throw new Error(`${name} is not open`) }

    const relSeq = BigInt(log.seq)

    await this._nextLog(data)
      .then((log) => log.append(data, seq))

    return { seq, data }
  }

  async appendBatch(data, seq=null) {
    const { path: name } = this
    let next = BigInt(this.seq) + 1n
    seq = seq !== null ? seq : next
    seq = parseBigInt(seq, new Error(`${name} seq must be string number`))
    if (next !== seq) { throw new Error(`${name} next ${next} !== ${seq}`) }
    if (!this._open) { throw new Error(`${name} is not open`) }
    const first = seq.toString()

    // todo
    return { seq: first, data }
  }

  async truncate(seq='-1') {
    const { path: name, encoder: enc } = this
    seq = parseBigInt(seq, new Error(`${name} seq must be string number`))
    if (seq < -1n) { throw new Error(`${name} seq must be >= -1`) }
    if (!this._open) { throw new Error(`${name} is not open`) }
    if (BigInt(this.seq) <= seq) { return }
    // todo
  }

  iter(seq='0', opts={}) {
    const { path: name } = this
    seq = parseBigInt(seq, new Error(`${name} seq must be string number`))
    if (seq < 0n) { throw new Error(`${name} seq must be >= 0`) }
    if (!this._open) { throw new Error(`${name} is not open`) }
    // todo
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

class MultiIterator {
  constructor(log, seq, opts={}) {
    opts = { name: 'iter', ...defaults, ...opts }
    this.name = opts.name
    this.path = `${log.path}-${opts.name}`
    if (opts.iterStepSize <= 0) { throw new Error(`${this.path} step size must be > 0`) }
    this.iterStepSize = opts.iterStepSize
    this._open = log.open()
    this.last = BigInt(log.seq)
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
          const meta = await enc.decodeMeta(buf, b)
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
          data = await enc.decodeBody(data)
          yield data
        }
      }

      if (next < (this.last + 1n)) {
        throw new Error(`${this.path} ended early - want ${this.last} got ${next}`)
      }

    } catch (err) {
      if (!err.message.includes(this.path)) {
        err.message = err.message.replace(this.log.path, this.path)
      }
      if (!err.message.includes(this.path)) {
        err.message = `${this.path} ${err.message}`
      }
      throw err
    }
  }
}

module.exports = { FsLog }
