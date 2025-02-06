const fs = require('fs')
const fsp = require('fs/promises')
const { Encoder } = require('./encoders.js')
const { FsLog } = require('./fslog.js')

const FS_MAX = 2 * (1024 * 1024 * 1024)
const buffer = require('node:buffer')
const BUF_MAX = buffer.kMaxLength

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
    if (err.code === 'ENOENT') { return true }
    throw new Error(`${fh.name} close error - ${err.message}`)
  })
}

const logFn = (multi, id) => {
  const name = `${multi.name}-m${id}`
  return new FsLog(multi.dir, name)
}

const logsFn = async (multi) => {
  const prefix = `${multi.name}-m`
  let files = await listDir(multi.dir)
  files = files.filter((name) => name.startsWith(prefix))
  let ids = files.map((name) => name.substring(prefix.length))
  ids = ids.filter((id) => id.endsWith('.log'))
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
    this._open = false
    this.fhlock = null
    this.logs = null
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
    if (buf.byteLength !== enc.multiLockLen) { throw new Error(`${fh.name} read len error`) }
    return enc.decodeMultiLock(buf, 0)
  }

  async _writeLock(arg1, arg2) {
    const { encoder: enc } = this
    const fh = arg2 !== undefined ? arg1 : this.fhlock
    const seq = arg2 !== undefined ? arg2 : arg1
    if (seq === null) { return fh.truncate(0) }
    const buf = Buffer.allocUnsafe(enc.multiLockLen)
    await enc.encodeMultiLock(buf, 0, seq)
    const write = async () => {
      const { bytesWritten } = await fh.write(buf, 0, buf.byteLength, 0).catch((err) => {throw new Error(`${fh.name} write error - ${err.message}`)})
      if (bytesWritten !== buf.byteLength) { throw new Error(`${fh.name} write len error`) }
    }
    return write()
  }

  async _handleResetLock() {
    const { path } = this
    let fhlock = null
    let logs = []
    try {

      // check lock
      fhlock = await openOrCreate(`${path}.lock`, 'r+')
      const seq = await this._readLock(fhlock)
      if (seq === null) { return }

      // truncate
      let sum = 0n
      logs = await this.logsFn(this)
      await Promise.all(logs.map((log) => log.start()))
      for (const log of logs) {
        const rel = (seq - sum).toString()
        await log.truncate(rel)
        sum += BigInt(log.seq)
      }

      // reset
      await this._writeLock(fhlock, null)
        .then(() => sync(fhlock))
        .then(() => sync(this.dir))

    } finally {
      const works = logs.map((log) => log.stop())
      works.push(close(fhlock))
      await Promise.all(works)
    }
  }

  async start() {
    if (this._open) { return }
    const works = []
    await this._handleResetLock()
    works.push(openOrCreate(`${this.path}.lock`, 'r+').then((fh) => this.fhlock = fh))
    works.push(this.logsFn(this))
    const [_, logs] = await Promise.all(works)

    if (logs.length <= 0) {
      this.logs = logs
      this.head = null
      this.seq = '-1'
      this._open = true
      return
    }

    await Promise.all(logs.map((log) => log.start()))
    logs.forEach((log, id) => log.mid = id)

    const sum = logs.map((log) => 1n + BigInt(log.seq))
      .map((s) => max(s, 0n))
      .reduce((acc, s) => acc + s, 0n)

    this.logs = logs
    const head = logs[logs.length-1]
    this.head = head.head
    this.seq = (sum - 1n).toString()
    this._open = true
  }

  stop() {
    if (!this._open) { return }
    const works = []
    this.iterators.forEach((iter) => iter.stop())
    this.iterators = []
    works.push(close(this.fhlock).then(() => this.fhlock = null))
    this.logs.forEach((log) => works.push(log.stop()))
    return Promise.all(works).then(() => {
      this.logs = this.head = this.seq = null
      this._open = false
    })
  }

  async _firstLog(data) {
    const first = this.logFn(this, 0)
    this.logs = [first]
    first.mid = 0
    await first.start()
    return first
  }

  async _nextLog(data) {
    if (this.logs.length <= 0) { return this._firstLog() }
    data = Array.isArray(data) ? data : [data]
    let log = this.logs[this.logs.length-1]
    const dataLen = data.reduce((acc, buf) => acc + buf.byteLength + log.encoder.bodyLen, 0)
    const logLen = log.offset + log.hlen
    const nextLen = max(logLen, 0n) + BigInt(dataLen)
    if (nextLen > this.maxLogLen) {
      const mid = log.mid + 1
      log = this.logFn(this, mid)
      this.logs.push(log)
      log.mid = mid
      await log.start()
    }
    return log
  }

  // todo: data not too big
  async append(data, seq=null) {
    const { path: name } = this
    let next = BigInt(this.seq) + 1n
    seq = seq !== null ? seq : next
    seq = parseBigInt(seq, new Error(`${name} seq must be string number`))
    if (next !== seq) { throw new Error(`${name} next ${next} !== ${seq}`) }
    if (!(data instanceof Buffer)) { throw new Error(`${name} data must be buffer`) }
    if (!this._open) { throw new Error(`${name} is not open`) }
    await this._writeLock(BigInt(this.seq))
      .then(() => sync(this.fhlock))
    const log = await this._nextLog(data)
    const logs = [...this.logs]
    logs.pop()
    const sum = logs.map((log) => 1n + BigInt(log.seq))
      .map((s) => max(s, 0n))
      .reduce((acc, s) => acc + s, 0n)
    const rel = (seq - sum).toString()
    const ok = await log.append(data, rel)
    await this._writeLock(null)
      .then(() => sync(this.fhlock))
    seq = seq.toString()
    this.seq = seq
    this.head = log.head
    return { seq, data: ok.data }
  }

  // todo: data not too big
  async appendBatch(data, seq=null) {
    const { path: name } = this
    let next = BigInt(this.seq) + 1n
    seq = seq !== null ? seq : next
    seq = parseBigInt(seq, new Error(`${name} seq must be string number`))
    if (next !== seq) { throw new Error(`${name} next ${next} !== ${seq}`) }
    if (!Array.isArray(data)) { throw new Error(`${name} data must be array`) }
    if (data.length <= 0) { throw new Error(`${name} data must be array with len > 0`) }
    if (!this._open) { throw new Error(`${name} is not open`) }
    await this._writeLock(BigInt(this.seq))
      .then(() => sync(this.fhlock))
    const log = await this._nextLog(data)
    const logs = [...this.logs]
    logs.pop()
    const sum = logs.map((log) => 1n + BigInt(log.seq))
      .map((s) => max(s, 0n))
      .reduce((acc, s) => acc + s, 0n)
    const rel = (seq - sum).toString()
    const ok = await log.appendBatch(data, rel)
    await this._writeLock(null)
      .then(() => sync(this.fhlock))
    this.seq = BigInt(this.seq) + BigInt(data.length)
    this.head = log.head
    return { seq: seq.toString(), data: ok.data }
  }

  async truncate(seq='-1') {
    const { path: name, encoder: enc } = this
    seq = parseBigInt(seq, new Error(`${name} seq must be string number`))
    if (seq < -1n) { throw new Error(`${name} seq must be >= -1`) }
    if (!this._open) { throw new Error(`${name} is not open`) }
    if (BigInt(this.seq) <= seq) { return }
    await this._writeLock(BigInt(seq))
      .then(() => sync(this.fhlock))
    let sum = 0n
    for (const log of this.logs) {
      const rel = (seq - sum).toString()
      await log.truncate(rel)
      sum += BigInt(log.seq)
    }
    await this._writeLock(null)
      .then(() => sync(this.fhlock))
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
    const logs = await this.logsFn(this)
    const works = [`${this.path}.lock`]
    return Promise.all(works.map(del))
      .then(() => Promise.all(works.map(sync)))
      .then(() => Promise.all(logs.map((log) => log.del())))
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

module.exports = { MultiFsLog }
