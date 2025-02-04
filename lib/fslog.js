const fs = require('fs')
const fsp = require('fs/promises')

const noop = () => {}
const max = (a, b) => a > b ? a : b
const min = (a, b) => a < b ? a : b

// todo: use timeouts
// todo: BigInt in truncate
// todo: no file contents in error messages

function timeout(ms, error) {
  let timer = null
  const timedout = new Promise((res, rej) => {
    const now = Date.now()
    let next = now + ms
    next = next - (next % 100)
    next = (100 + next) - now
    timer = setTimeout(rej, next, error)
  })
  return [timer, timedout]
}

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
      return fsp.writeFile(path, Buffer.alloc(0))
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

// todo: accept BigInt
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
    if (err.code === 'ENOENT') { return false }
    throw new Error(`${fh.name} close error - ${err.message}`)
  })
}

const defaults = {
  logTimeout: 1_500,
  iterStepSize: 1024,
  rollbackCb: noop,
  rollForwardCb: noop,
}

class FsLog {
  constructor(dir, name, opts={}) {
    opts = { ...defaults, ...opts }
    this.logTimeout = opts.logTimeout
    this.rollbackCb = opts.rollbackCb
    this.rollForwardCb = opts.rollForwardCb
    this.dir = dir
    this.name = name
    this.path = dir + name
    this._open = false
    this.fhlog = null
    this.fhoff = null
    this.fhlock = null
    this.offset = null
    this.olen = null
    this.hlen = null
    this.head = null
    this.seq = null
    this.iterators = []
  }

  open() {
    return this._open
  }

  async _readLock(fh) {
    fh = fh ? fh : this.fhlock
    const buf = await fh.readFile().catch((err) => {throw new Error(`${fh.name} read error - ${err.message}`)})
    if (buf.byteLength <= 0) { return null }
    if (buf.byteLength !== 16) { throw new Error(`${fh.name} read len != 16 error`) }
    const olen = buf.readBigInt64BE(0)
    const llen = buf.readBigInt64BE(8)
    return { olen, llen }
  }

  async _writeLock(arg1, arg2, arg3) {
    const fh = arg3 !== undefined ? arg1 : this.fhlock
    const olen = arg3 !== undefined ? arg2 : arg1
    const llen = arg3 !== undefined ? arg3 : arg2
    if (olen === null || llen == null) { return truncate(fh, 0) }
    const buf = Buffer.alloc(16)
    buf.writeBigInt64BE(olen)
    buf.writeBigInt64BE(llen, 8)
    const write = async () => {
      const { bytesWritten } = await fh.write(buf, 0, buf.byteLength, 0).catch((err) => {throw new Error(`${fh.name} write error - ${err.message}`)})
      if (bytesWritten !== buf.byteLength) { throw new Error(`${fh.name} write len error`) }
    }
    return write()
  }

  async _appendOffset(seq, off, len) {
    const { fhoff: fh } = this
    const buf = Buffer.alloc(24)
    buf.writeBigInt64BE(seq)
    buf.writeBigInt64BE(off, 8)
    buf.writeBigInt64BE(len, 16)
    const write = async () => {
      const { bytesWritten } = await fh.write(buf).catch((err) => {throw new Error(`${fh.name} write error - ${err.message}`)})
      if (bytesWritten !== buf.byteLength) { throw new Error(`${fh.name} write len error`) }
      return BigInt(buf.byteLength)
    }
    return write()
  }

  async _appendOffsets(offs) {
    const { fhoff: fh } = this
    let o = 0
    const buf = Buffer.alloc(24 * offs.length)
    offs.forEach((off) => {
      buf.writeBigInt64BE(off.seq, o)
      buf.writeBigInt64BE(off.off, o + 8)
      buf.writeBigInt64BE(off.len, o + 16)
      o += 24
    })
    const write = async () => {
      const { bytesWritten } = await fh.write(buf).catch((err) => {throw new Error(`${fh.name} write error - ${err.message}`)})
      if (bytesWritten !== buf.byteLength) { throw new Error(`${fh.name} write len error`) }
      return BigInt(buf.byteLength)
    }
    return write()
  }

  async _readOffset() {
    const { fhoff: fh } = this
    const { size } = await fh.stat({bigint: true}).catch((err) => {throw new Error(`${fh.name} stat error - ${err.message}`)})
    if (size <= 0n) { return null }
    const buf = Buffer.alloc(24)
    const pos = size - BigInt(24)
    if (size < buf.byteLength) { throw new Error(`${fh.name} read size < buf len error`) }
    const { bytesRead } = await read(fh, buf, 0, 24, pos).catch((err) => {throw new Error(`${fh.name} read error - ${err.message}`)})
    if (bytesRead !== buf.byteLength) { throw new Error(`${fh.name} read len error`) }
    const seq = buf.readBigInt64BE(0)
    const off = buf.readBigInt64BE(8)
    const hlen = buf.readBigInt64BE(16)
    return { seq, off, hlen, olen: size }
  }

  async _readOffsets(seq, count) {
    const { fhoff: fh } = this
    const pos = 24n * seq
    count = 24 * count
    const buf = Buffer.alloc(count)
    const { bytesRead } = await read(fh, buf, 0, count, pos).catch((err) => {throw new Error(`${fh.name} read error - ${err.message}`)})
    if ((bytesRead % 24) !== 0) { throw new Error(`${fh.name} read mod 24 error`) }
    return buf.slice(0, bytesRead)
  }

  _appendLog(data) {
    const { fhlog: fh } = this
    const len = BigInt(data.byteLength)
    const write = async () => {
      const { bytesWritten } = await fh.write(data).catch((err) => {throw new Error(`${fh.name} write error - ${err.message}`)})
      if (bytesWritten !== Number(len)) { throw new Error(`${fh.name} write len error`) }
    }
    return { len, write }
  }

  _appendLogs(data) {
    const { fhlog: fh } = this
    const lens = data.map((buf) => BigInt(buf.byteLength))
    data = Buffer.concat(data)
    const write = async () => {
      const { bytesWritten } = await fh.write(data).catch((err) => {throw new Error(`${fh.name} write error - ${err.message}`)})
      if (bytesWritten !== data.byteLength) { throw new Error(`${fh.name} write len error`) }
    }
    return { lens, write }
  }

  async _readHead(arg1, arg2) {
    const fh = arg2 !== undefined ? arg1 : this.fhlog
    const offset = arg2 !== undefined ? arg2 : arg1
    const { size } = await fh.stat({bigint: true}).catch((err) => {throw new Error(`${fh.name} stat error - ${err.message}`)})
    const len = Number(size - offset)
    if (len < 0) { throw new Error(`${fh.name} read len < 0 error`) }
    const buf = Buffer.alloc(len)
    const { bytesRead } = await read(fh, buf, 0, len, offset).catch((err) => {throw new Error(`${fh.name} read error - ${err.message}`)})
    if (bytesRead !== len) { throw new Error(`${fh.name} read len error`) }
    return buf
  }

  async _readLogs(offset, len) {
    const { fhlog: fh } = this
    len = Number(len)
    const buf = Buffer.alloc(len)
    const { bytesRead } = await read(fh, buf, 0, len, offset).catch((err) => {throw new Error(`${fh.name} read error - ${err.message}`)})
    if (bytesRead !== len) { throw new Error(`${fh.name} read len error`) }
    return buf
  }

  // last operation = append = rollback
  // last operation = truncate = finish apply
  async _handleResetLock() {
    const handles = []
    const { path } = this

    try {

      // check lock
      const fhlock = await openOrCreate(`${path}.lock`, 'r+')
      handles.push(fhlock)
      const lock = await this._readLock(fhlock)
      // nothing to do
      if (lock === null) { return }

      // work
      const fhlog = await openOrCreate(`${path}.log`, 'r+')
      handles.push(fhlog)
      const fhoff = await openOrCreate(`${path}.off`, 'r+')
      handles.push(fhoff)

      let works = []
      const { olen, llen } = lock
      works.push(truncate(fhoff, olen))
      works.push(truncate(fhlog, llen))
      await Promise.all(works)

      works = []
      works.push(sync(fhlog))
      works.push(sync(fhoff))
      await Promise.all(works)

      // reset
      await this._writeLock(fhlock, null, null)
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
        this.olen = -1n
        this.hlen = -1n
        this.head = null
        this.seq = '-1'
        this._open = true
        return
      }
      const { seq, off, hlen, olen } = ok
      return this._readHead(off).then((head) => {
        if (hlen !== BigInt(head.byteLength)) {
          throw new Error(`${path} offset file says hlen = ${hlen} but got ${head.byteLength}`)
        }
        this.offset = off
        this.olen = olen
        this.hlen = hlen
        this.head = head
        this.seq = seq.toString()
        this._open = true
      })
    })
  }

  stop() {
    const works = []
    works.push(close(this.fhlog).then(() => this.fhlog = null))
    works.push(close(this.fhoff).then(() => this.fhoff = null))
    works.push(close(this.fhlock).then(() => this.fhlock = null))
    this.iterators.forEach((iter) => iter.stop())
    this.iterators = []
    return Promise.all(works).then(() => {
      this.offset = this.olen = this.hlen = this.head = this.seq = null
      this._open = false
    })
  }

  async append(data, seq=null) {
    const { path: name } = this
    let next = BigInt(this.seq) + 1n
    seq = seq !== null ? seq : next
    seq = parseBigInt(seq, new Error(`${name} seq must be string number`))
    if (next !== seq) { throw new Error(`${name} next ${next} !== ${seq}`) }
    if (!(data instanceof Buffer)) { throw new Error(`${name} data must be buffer`) }
    if (!this._open) { throw new Error(`${name} is not open`) }

    // state to lock
    const olen = max(this.olen, 0n)
    const llen = max(this.offset + this.hlen, 0n)
    await this._writeLock(olen, llen)
      .then(() => sync(this.fhlock))

    // work
    let works = []
    const { len, write } = this._appendLog(data)
    works.push(write())
    const hlen = max(this.hlen, 0n)
    const offset = max(this.offset, 0n) + hlen
    works.push(this._appendOffset(seq, offset, len))
    const [_, olenn] = await Promise.all(works)

    // flush
    works = []
    works.push(sync(this.fhlog))
    works.push(sync(this.fhoff))
    await Promise.all(works)
    seq = seq.toString()
    this.rollbackCb(seq)

    // reset lock
    await this._writeLock(null, null)
      .then(() => sync(this.fhlock))

    // ok
    this.offset = offset
    this.olen = olen + olenn
    this.hlen = len
    this.head = data
    this.seq = seq
    return { seq, data }
  }

  async appendBatch(data, seq=null) {
    const { path: name } = this
    let next = BigInt(this.seq) + 1n
    seq = seq !== null ? seq : next
    seq = parseBigInt(seq, new Error(`${name} seq must be string number`))
    if (next !== seq) { throw new Error(`${name} next ${next} !== ${seq}`) }
    if (!Array.isArray(data)) { throw new Error(`${name} data must be array`) }
    if (data.length <= 0) { throw new Error(`${name} data must be array with len > 0`) }
    if (!this._open) { throw new Error(`${name} is not open`) }
    const first = seq.toString()

    // state to lock
    const olen = max(this.olen, 0n)
    const llen = max(this.offset + this.hlen, 0n)
    await this._writeLock(olen, llen)
      .then(() => sync(this.fhlock))

    // write data
    let works = []
    const { lens, write } = this._appendLogs(data)
    works.push(write())

    // write offsets
    const hlen = max(this.hlen, 0n)
    let offset = max(this.offset, 0n) + hlen
    next = lens.map((len) => {
      const next = { seq, off: offset, len }
      offset += len
      seq += 1n
      return next
    })
    works.push(this._appendOffsets(next))
    const [_, olenn] = await Promise.all(works)

    // flush
    works = []
    works.push(sync(this.fhlog))
    works.push(sync(this.fhoff))
    await Promise.all(works)
    seq = (seq - 1n).toString()
    this.rollbackCb(seq)

    // reset lock
    await this._writeLock(null, null)
      .then(() => sync(this.fhlock))

    // ok
    next = next[next.length-1]
    this.offset = next.off
    this.olen = olen + olenn
    this.hlen = next.len
    this.head = data[data.length-1]
    this.seq = seq
    return { first, last: seq, data }
  }

  async truncate(seq='-1') {
    const { path: name } = this
    seq = parseBigInt(seq, new Error(`${name} seq must be string number`))
    if (seq < -1n) { throw new Error(`${name} seq must be >= -1`) }
    if (!this._open) { throw new Error(`${name} is not open`) }
    if (BigInt(this.seq) <= seq) { return }

    this.iterators.filter((iter) => iter.last > seq).forEach((iter) => iter.stop())
    this.iterators = this.iterators.filter((iter) => !(iter.last > seq))

    let works = []
    if (seq === -1n) {
      await this._writeLock(0n, 0n)
        .then(() => sync(this.fhlock))
      works.push(truncate(this.fhoff, 0n))
      works.push(truncate(this.fhlog, 0n))
      await Promise.all(works)
      works = []
      works.push(sync(this.fhoff))
      works.push(sync(this.fhlog))
      await Promise.all(works)
      this.rollForwardCb('-1')
      await this._writeLock(null, null)
        .then(() => sync(this.fhlock))
      this.offset = -1n
      this.olen = -1n
      this.hlen = -1n
      this.head = null
      this.seq = '-1'
      return
    }

    const buf = await this._readOffsets(seq, 1)
    const off = buf.readBigInt64BE(8)
    const hlen = buf.readBigInt64BE(16)
    const olen = 24n * (1n + seq)
    const llen = off + hlen
    await this._writeLock(olen, llen)
      .then(() => sync(this.fhlock))

    works.push(truncate(this.fhoff, olen))
    works.push(truncate(this.fhlog, llen))
    await Promise.all(works)

    works = []
    works.push(sync(this.fhlog))
    works.push(sync(this.fhoff))
    await Promise.all(works)

    seq = seq.toString()
    this.rollForwardCb(seq)
    await this._writeLock(null, null)
      .then(() => sync(this.fhlock))

    return this._readHead(off).then((head) => {
      this.offset = off
      this.olen = olen
      this.hlen = hlen
      this.head = head
      this.seq = seq
    })
  }

  iter(seq='0', opts={}) {
    const { path: name } = this
    seq = parseBigInt(seq, new Error(`${name} seq must be string number`))
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
    this.last = BigInt(log.seq)
    this.log = log
    this.seq = seq
  }

  stop() {
    this._open = false
  }

  async *lazy() {
    const { log, iterStepSize, last } = this
    let next = this.seq

    try {
      while (next <= last) {
        const works = []
        if (!this._open) { throw new Error(`${this.path} is not open`) }
        let buf = await log._readOffsets(next, iterStepSize)
        if (!this._open) { throw new Error(`${this.path} is not open`) }

        for (let b = 0; b < buf.byteLength; b += 24) {
          const seq = buf.readBigInt64BE(b + 0)
          const off = buf.readBigInt64BE(b + 8)
          const len = buf.readBigInt64BE(b + 16)
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
          const data = buf.slice(off, off + len)
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

module.exports = {
  FsLog
}
