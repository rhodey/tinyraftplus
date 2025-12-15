const fs = require('fs')
const fsp = require('fs/promises')
const { Encoder } = require('./encoder.js')

const {
  openOrCreate,
  stat,
  read,
  trim,
  sync,
  close,
  del
} = require('./fs.js')

const noop = () => {}
const ready = Promise.resolve()
const max = (a, b) => a > b ? a : b
const min = (a, b) => a < b ? a : b

const defaults = {
  // fetch records in batches of N when iterating
  iterStepSize: 1024,
  // default encoder
  encoder: new Encoder(),
  // ignore
  rollbackCb: noop,
  rollForwardCb: noop,
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
    this.fhlock = null
    this.fhoff = null
    this.fhlog = null
    this.offset = null
    this.hlen = null
    this.head = null
    this.seq = null
    this._txn = ready
    this._closing = null
  }

  get isOpen() {
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
    if (seq === null) { return trim(fh, 0) }
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
    const { size } = await stat(fh, {bigint: true}).catch((err) => {throw new Error(`${fh.name} stat error - ${err.message}`)})
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
    return buf.subarray(0, bytesRead)
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
    const { size } = await stat(fh, {bigint: true}).catch((err) => {throw new Error(`${fh.name} stat error - ${err.message}`)})
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

  // trim log if needed and reset lock
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
      await trim(fhoff, olen)
      const fhlog = await openOrCreate(`${path}.log`, 'r+')
      handles.push(fhlog)

      if (seq >= 0n) {
        const off = await this._readOffset(fhoff)
        const { seq: seq2, off: offset, hlen } = off
        if (seq !== seq2) { throw new Error(`${path} lock seq ${seq} and offset seq ${seq2} do not agree`) }
        const llen = offset + hlen
        await trim(fhlog, llen)
      } else {
        await trim(fhlog, 0n)
      }

      const works = []
      works.push(sync(fhoff))
      works.push(sync(fhlog))
      await Promise.all(works)
      await this._writeLock(fhlock, null)
        .then(() => sync(fhlock))
        .then(() => sync(this.dir))

    } catch (err) {
      throw new Error(`${path} failed to reset - ${err.message}`)
    } finally {
      await Promise.all(handles.map(close))
    }
  }

  // prevents double open
  open(reset=false) {
    const { path } = this
    if (this._closing) { return Promise.reject(new Error(`${path} is not open`)) }
    if (!reset && this._open) { return ready }
    if (!reset && this._opening) { return this._opening }
    const opening = new Promise(async (res, rej) => {
      try {

        const works = []
        await this._handleResetLock()
        !reset && works.push(openOrCreate(`${path}.lock`, 'r+').then((fh) => this.fhlock = fh))
        !reset && works.push(openOrCreate(`${path}.off`, 'a+').then((fh) => this.fhoff = fh))
        !reset && works.push(openOrCreate(`${path}.log`, 'a+').then((fh) => this.fhlog = fh))

        !reset && await Promise.all(works).then((arr) => {
          // newly created files need this
          return Promise.all(arr.map(sync))
            .then(() => sync(this.dir))
        })

        await this._readOffset().then((ok) => {
          if (ok === null || ok.off <= -1n || ok.seq <= -1n) {
            this.offset = -1n
            this.hlen = -1n
            this.head = null
            this.seq = -1n
            !reset && (this._open = true)
            return
          }
          const { seq, off, hlen } = ok
          return this._readHead(off).then((head) => {
            this.offset = off
            this.hlen = hlen
            this.head = head
            this.seq = seq
            !reset && (this._open = true)
          })
        })
        !reset && (this._opening = null)
        res()

      } catch (err) {
        !reset && (this._opening = null)
        rej(err)
      }
    })

    !reset && (this._opening = opening)
    return opening
  }

  // waits for txn
  close() {
    if (!this._open) { return ready }
    if (this._closing) { return this._closing }
    this.iterators.forEach((iter) => iter.close())
    this.iterators = []
    const closee = () => new Promise((res, rej) => {
      const works = []
      this.iterators.forEach((iter) => iter.close())
      this.iterators = []
      works.push(close(this.fhlock).then(() => this.fhlock = null))
      works.push(close(this.fhoff).then(() => this.fhoff = null))
      works.push(close(this.fhlog).then(() => this.fhlog = null))
      return Promise.all(works).then(() => {
        this.offset = this.hlen = this.head = this.seq = null
        this._open = false
        this._closing = null
        res()
      }).catch((err) => {
        this._closing = null
        rej(err)
      })
    })
    return this._closing = this._txn.catch(noop).then(closee)
  }

  async txn(seq=undefined) {
    const { path } = this
    if (!this._open || this._closing) { throw new Error(`${path} is not open`) }
    let cb = null
    const delay = new Promise((res, _) => cb = res)
    const callOrThrow = (fn) => {
      if (!cb) { return Promise.reject(new Error(`${path} txn already commit or abort`)) }
      return fn()
    }
    const callOnce = (fn) => {
      if (!cb) { return Promise.reject(new Error(`${path} txn already commit or abort`)) }
      return fn().then((ok) => {
        // todo: maybe ignore
        if (!cb) { return Promise.reject(new Error(`${path} txn already commit or abort`)) }
        cb(); cb = null
        return ok
      })
    }
    const lock = (seq) => this._writeLock(seq).then(() => sync(this.fhlock))
    const api = this._txn.catch(noop).then(() => {
      if (seq === null) {
        return ready
      } else if (seq === undefined) {
        return lock(this.seq)
      } else {
        return lock(seq)
      }
    }).then(() => {
      const commit = () => seq !== null ? this._commit() : ready
      const abort = () => seq !== null ? this._abort() : ready
      return {
        append: (data, seq) => callOrThrow(() => this.append(data, seq, true)),
        appendBatch: (data, seq) => callOrThrow(() => this.appendBatch(data, seq, true)),
        lock: (seq) => callOrThrow(() => lock(seq)),
        _commit: () => callOrThrow(() => commit().then(() => cb)),
        commit: () => callOnce(commit),
        abort: () => callOnce(abort),
      }
    }).catch((err) => { cb(); throw err })
    this._txn = this._txn.catch(noop).then(() => delay)
    return api
  }

  _commit() {
    const works = []
    works.push(sync(this.fhoff))
    works.push(sync(this.fhlog))
    return Promise.all(works)
      .then(() => this._writeLock(null))
      .then(() => sync(this.fhlock))
  }

  _abort() {
    const works = []
    works.push(sync(this.fhoff))
    works.push(sync(this.fhlog))
    return Promise.all(works).then(() => this.open(true)).catch((err) => {
      this.close().catch(noop)
      throw new Error(`${this.path} failed to abort - ${err.message}`)
    })
  }

  async append(data, seq=null, _txn=false) {
    const parseArgs = () => {
      const { path } = this
      const next = this.seq + 1n
      seq = seq !== null ? seq : next
      if (next !== seq) { throw new Error(`${path} next ${next} !== ${seq}`) }
      if (!Buffer.isBuffer(data)) { throw new Error(`${path} data must be buffer`) }
    }

    // auto txn
    let cb = noop
    _txn !== true && (_txn = await this.txn())
    parseArgs()

    try {

      // work
      const works = []
      const { len, write } = this._appendLog(data)
      works.push(write())
      const hlen = max(this.hlen, 0n)
      const offset = max(this.offset, 0n) + hlen
      works.push(this._appendOffset(seq, offset, len))
      await Promise.all(works)
      this.rollbackCb(seq)

      // auto txn
      _txn !== true && (cb = await _txn._commit())

      // ok
      this.offset = offset
      this.hlen = len
      this.head = data
      this.seq = seq
      cb()

    } catch (err) {
      // auto txn
      _txn !== true && await _txn.abort().catch(noop)
      throw err
    }

    return seq
  }

  async appendBatch(data, seq=null, _txn=false) {
    const parseArgs = () => {
      const { path } = this
      const next = this.seq + 1n
      seq = seq !== null ? seq : next
      if (next !== seq) { throw new Error(`${path} next ${next} !== ${seq}`) }
      if (!Array.isArray(data)) { throw new Error(`${path} data must be array`) }
      if (data.length <= 0) { throw new Error(`${path} data must be array with len > 0`) }
      if (!data.every((buf) => Buffer.isBuffer(buf))) { throw new Error(`${path} data must be array of bufs`) }
      return seq
    }

    // auto txn
    let cb = noop
    _txn !== true && (_txn = await this.txn())
    const first = parseArgs()

    try {

      // work
      const works = []
      const { lens, write } = this._appendLogs(data)
      works.push(write())

      // work
      const hlen = max(this.hlen, 0n)
      let offset = max(this.offset, 0n) + hlen
      let next = lens.map((len) => {
        const next = { seq, off: offset, len }
        offset += len
        seq += 1n
        return next
      })
      works.push(this._appendOffsets(next))
      await Promise.all(works)
      this.rollbackCb(--seq)

      // auto txn
      _txn !== true && (cb = await _txn._commit())

      // ok
      next = next[next.length-1]
      this.offset = next.off
      this.hlen = next.len
      this.head = data[data.length-1]
      this.seq = seq
      cb()

    } catch (err) {
      // auto txn
      _txn !== true && await _txn.abort().catch(noop)
      throw err
    }

    return first
  }

  // trim is fail forward / cannot be supported by txn api
  // txn = true = return a txn to caller to chain
  async trim(seq=-1n, txn=false) {
    const parseArgs = () => {
      const { path } = this
      if (typeof seq !== 'bigint') { throw new Error(`${path} seq must be big int`) }
      if (seq < -1n) { throw new Error(`${path} seq must be >= -1`) }
      if (this.seq <= seq) { return true }
    }

    this.iterators.filter((iter) => iter.last > seq).forEach((iter) => iter.close())
    this.iterators = this.iterators.filter((iter) => iter._open)

    let _txn = null
    if (txn === false || txn === true) {
      _txn = await this.txn(seq)
    } else {
      _txn = txn
    }

    this.iterators.filter((iter) => iter.last > seq).forEach((iter) => iter.close())
    this.iterators = this.iterators.filter((iter) => iter._open)

    const nothing = parseArgs() === true
    if (nothing && txn) {
      return _txn
    } else if (nothing) {
      await _txn.commit()
      return
    }

    try {

      const works = []
      if (seq === -1n) {
        works.push(trim(this.fhoff, 0n))
        works.push(trim(this.fhlog, 0n))
        await Promise.all(works)
        this.rollForwardCb(-1n)
        this.offset = -1n
        this.hlen = -1n
        this.head = null
        this.seq = -1n
        if (txn) { return _txn }
        await _txn.commit()
        return
      }

      const { encoder: enc } = this
      const buf = await this._readOffsets(seq, 1)
      const meta = await enc.decodeMeta(this, buf, 0)
      const { off, len: hlen } = meta
      const olen = BigInt(enc.metaLen) * (seq + 1n)
      const llen = off + hlen
      works.push(trim(this.fhoff, olen))
      works.push(trim(this.fhlog, llen))
      await Promise.all(works)
      this.rollForwardCb(seq)

      await this._readHead(off).then((head) => {
        this.offset = off
        this.hlen = hlen
        this.head = head
        this.seq = seq
      })

      if (txn) { return _txn }
      await _txn.commit()

    } catch (err) {
      if (!txn) { await _txn.abort().catch(noop) }
      throw err
    }
  }

  iter(seq=0n, opts={}) {
    const { path } = this
    // todo: why opts.txn
    if (!opts.txn && (!this._open || this._closing)) { throw new Error(`${path} is not open`) }
    if (typeof seq !== 'bigint') { throw new Error(`${path} seq must be big int`) }
    if (seq < 0n) { throw new Error(`${path} seq must be >= 0`) }
    const iter = new FsIterator(this, seq, opts)
    this.iterators = this.iterators.filter((iter) => iter._open)
    this.iterators.push(iter)
    return opts.clazz ? iter : iter.lazy()
  }

  async del() {
    const { path } = this
    if (this._open) { throw new Error(`${path} is open`) }
    const works = [`${path}.log`, `${path}.off`, `${path}.lock`]
    return Promise.all(works.map(del))
      .then(() => Promise.all(works.map(sync)))
      .then(() => sync(this.dir))
  }
}

class FsIterator {
  constructor(log, seq, opts={}) {
    opts = { name: 'iter', ...defaults, ...opts }
    this.path = `${log.path}-${opts.name}`
    if (opts.iterStepSize <= 0) { throw new Error(`${this.path} step size must be > 0`) }
    this.iterStepSize = opts.iterStepSize
    this._open = true
    this.opts = opts
    this.txn = opts.txn
    this.seq = seq
    this.log = log
    this.last = log.seq
  }

  close() {
    this._open = false
    if (this.opts.txn) { return }
    this.txn && this.txn.commit().catch(noop)
  }

  async *lazy() {
    const { path, log, iterStepSize, last } = this
    const { encoder: enc } = log
    let next = this.seq

    try {

      if (!this._open) { return }
      this.txn = this.txn ?? await log.txn(null)

      while (next <= last) {
        if (!this._open) { return }
        const works = []
        let buf = await log._readOffsets(next, iterStepSize)

        for (let b = 0; b < buf.byteLength; b += enc.metaLen) {
          if (!this._open) { return }
          const meta = await enc.decodeMeta(this, buf, b)
          const { seq, off, len } = meta
          if (seq !== next) { throw new Error(`${path} seq out of order in read offsets loop`) }
          works.push([off, len])
          next = seq + 1n
          if (next > last) { break }
        }
        if (!this._open) { return }
        if (works.length <= 0) { return }

        const begin = works[0][0]
        let end = works[works.length-1]
        end = end[0] + end[1]
        buf = await log._readLogs(begin, end - begin)

        for (const work of works) {
          if (!this._open) { return }
          let [off, len] = work
          off = Number(off - begin)
          len = Number(len)
          let data = buf.subarray(off, off + len)
          data = await enc.decodeBody(this, data)
          if (!this._open) { return }
          yield data
        }
      }

    } catch (err) {
      if (!err.message.includes(path)) {
        err.message = `${path} ${err.message}`
      }
      throw err
    } finally {
      this.close()
    }
  }
}

module.exports = { FsLog }
