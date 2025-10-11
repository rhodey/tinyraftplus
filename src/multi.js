const { Encoder } = require('./encoder.js')
const { FsLog } = require('./fslog.js')

const {
  listDir,
  openOrCreate,
  trim,
  sync,
  close,
  del
} = require('./fs.js')

const noop = () => {}
const ready = Promise.resolve()

const max = (a, b) => a > b ? a : b
const min = (a, b) => a < b ? a : b

const logFn = async (multi, id) => {
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
  // fetch records in batches of N when iterating
  iterStepSize: 1024,
  // single log max byte length
  maxLogLen: 1024 * 1024 * 1024,
  // default encoder
  encoder: new Encoder(),
  // how to create logs
  logFn, logsFn,
  // ignore
  rollbackCb: noop,
  rollForwardCb: noop,
}

// a big log composed of smaller logs
class MultiFsLog {
  constructor(dir, name, opts={}) {
    opts = { ...defaults, ...opts }
    this.rollbackCb = opts.rollbackCb
    this.rollForwardCb = opts.rollForwardCb
    this.maxLogLen = BigInt(opts.maxLogLen)
    this.encoder = opts.encoder
    this.logFn = opts.logFn
    this.logsFn = opts.logsFn
    this.dir = dir
    this.name = name
    this.path = dir + name
    this.iterators = []
    this._open = false
    this.fhlock = null
    this.logs = null
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

  // trim logs if needed and reset lock
  async _handleResetLock() {
    const { path } = this
    let fhlock = null
    let logs = []
    try {

      fhlock = await openOrCreate(`${path}.lock`, 'r+')
      const seq = await this._readLock(fhlock)
      if (seq === null) { return }

      let sum = 0n
      logs = await this.logsFn(this)
      logs = await Promise.all(logs)
      await Promise.all(logs.map((log) => log.open()))
      for (const log of logs) {
        const rel = seq - sum
        await log.trim(rel)
        sum += log.seq + 1n
      }

      const empty = logs.filter((log) => log.seq < 0n)
      await Promise.all(empty.map((log) => log.close()))
      await Promise.all(empty.map((log) => log.del()))

      await this._writeLock(fhlock, null)
        .then(() => sync(fhlock))
        .then(() => sync(this.dir))

    } catch (err) {
      throw new Error(`${path} failed to reset - ${err.message}`)
    } finally {
      const works = logs.map((log) => log.close())
      works.push(close(fhlock))
      await Promise.all(works)
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

        let logs = this.logs
        await this._handleResetLock()

        if (!reset) {
          const works = []
          works.push(openOrCreate(`${path}.lock`, 'r+').then((fh) => this.fhlock = fh))
          works.push(this.logsFn(this))
          logs = await Promise.all(works).then((arr) => arr[1])
          logs = await Promise.all(logs)
          await Promise.all(logs.map((log) => log.open()))
          logs.forEach((log, idx) => log.mid = idx)
        } else {
          await Promise.all(logs.map((log) => log.open(true)))
          const empty = logs.filter((log) => log.seq < 0n)
          await Promise.all(empty.map((log) => log.close()))
          await Promise.all(empty.map((log) => log.del()))
          logs = logs.filter((log) => log.isOpen)
        }

        if (logs.length <= 0) {
          this.logs = logs
          this.head = null
          this.seq = -1n
          !reset && (this._open = true)
          !reset && (this._opening = null)
          return res()
        }

        const sum = logs.map((log) => 1n + log.seq)
          .map((s) => max(s, 0n))
          .reduce((acc, s) => acc + s, 0n)

        this.logs = logs
        const head = logs[logs.length-1]
        this.head = head.head
        this.seq = sum - 1n
        !reset && (this._open = true)
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
      this.logs.forEach((log) => works.push(log.close()))
      return Promise.all(works).then(() => {
        this.logs = this.head = this.seq = null
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

  async txn(seq=undefined, _txns={}) {
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
        if (!cb) { return Promise.reject(new Error(`${path} txn already commit or abort`)) }
        cb(); cb = null
        return ok
      })
    }
    const lock = (seq) => this._writeLock(seq).then(() => sync(this.fhlock))
    const api = this._txn.catch(noop).then(() => {
      if (seq === undefined) {
        return lock(this.seq)
      } else if (seq === null) {
        return ready
      } else {
        return lock(seq)
      }
    }).then(() => {
      const commit = () => seq !== null ? this._commit(_txns) : ready
      const abort = () => seq !== null ? this._abort(_txns) : ready
      return {
        append: (data, seq) => callOrThrow(() => this.append(data, seq, true, _txns)),
        appendBatch: (data, seq) => callOrThrow(() => this.appendBatch(data, seq, true, _txns)),
        lock: (seq) => callOrThrow(() => lock(seq)),
        _commit: () => callOrThrow(() => commit().then(() => cb)),
        commit: () => callOnce(commit),
        abort: () => callOnce(abort),
      }
    }).catch((err) => { cb(); throw err })
    this._txn = this._txn.catch(noop).then(() => delay)
    return api
  }

  _commit(_txns) {
    _txns = Object.values(_txns)
    return Promise.all(_txns.map((txn) => txn.commit()))
      .then(() => this._writeLock(null))
      .then(() => sync(this.fhlock))
  }

  _abort(_txns) {
    _txns = Object.values(_txns)
    _txns = _txns.map((txn) => txn.abort())
    return Promise.all(_txns).then(() => this.open(true)).catch((err) => {
      this.close().catch(noop)
      throw new Error(`${this.path} failed to abort - ${err.message}`)
    })
  }

  async _firstLog(data) {
    const first = await this.logFn(this, 0)
    this.logs = [first]
    first.mid = 0
    await first.open()
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
      log = await this.logFn(this, mid)
      this.logs.push(log)
      log.mid = mid
      await log.open()
    }
    return log
  }

  _safeLen(data) {
    data = Array.isArray(data) ? data : [data]
    const dataLen = data.reduce((acc, buf) => acc + buf.byteLength + this.encoder.bodyLen, 0)
    if (BigInt(dataLen) <= this.maxLogLen) { return }
    throw new Error(`${this.path} data len ${dataLen} > max ${this.maxLogLen}`)
  }

  async _pruneEmpty() {
    const empty = this.logs.filter((log) => log.seq < 0n)
    await Promise.all(empty.map((log) => log.close()))
    await Promise.all(empty.map((log) => log.del()))
    this.logs = this.logs.filter((log) => log.isOpen)
  }

  async append(data, seq=null, _txn=false, _txns={}) {
    const parseArgs = () => {
      const { path } = this
      const next = this.seq + 1n
      seq = seq !== null ? seq : next
      if (next !== seq) { throw new Error(`${path} next ${next} !== ${seq}`) }
      if (!(data instanceof Buffer)) { throw new Error(`${path} data must be buffer`) }
      this._safeLen(data)
    }

    // auto txn
    let cb = noop
    _txn !== true && (_txn = await this.txn(undefined, _txns))
    parseArgs()

    try {

      const log = await this._nextLog(data)
      _txns[log.mid] = _txns[log.mid] ?? await log.txn()

      const logs = [...this.logs]
      logs.pop()
      const sum = logs.map((log) => 1n + log.seq)
        .map((s) => max(s, 0n))
        .reduce((acc, s) => acc + s, 0n)
      const rel = seq - sum

      await log.append(data, rel, true)
      this.rollbackCb(seq)

      // auto txn
      _txn !== true && (cb = await _txn._commit())
      this.head = log.head
      this.seq = seq
      cb()

    } catch (err) {
      // auto txn
      _txn !== true && await _txn.abort().catch(noop)
      await this._pruneEmpty().catch(noop)
      throw err
    }

    return seq
  }

  async appendBatch(data, seq=null, _txn=false, _txns={}) {
    const parseArgs = () => {
      const { path } = this
      const next = this.seq + 1n
      seq = seq !== null ? seq : next
      if (next !== seq) { throw new Error(`${path} next ${next} !== ${seq}`) }
      if (!Array.isArray(data)) { throw new Error(`${path} data must be array`) }
      if (data.length <= 0) { throw new Error(`${path} data must be array with len > 0`) }
      this._safeLen(data)
      return seq
    }

    // auto txn
    let cb = noop
    _txn !== true && (_txn = await this.txn(undefined, _txns))
    const first = parseArgs()

    try {

      const log = await this._nextLog(data)
      _txns[log.mid] = _txns[log.mid] ?? await log.txn()

      const logs = [...this.logs]
      logs.pop()
      const sum = logs.map((log) => 1n + log.seq)
        .map((s) => max(s, 0n))
        .reduce((acc, s) => acc + s, 0n)
      const rel = seq - sum

      await log.appendBatch(data, rel, true)
      seq = this.seq + BigInt(data.length)
      this.rollbackCb(seq)

      // auto txn
      _txn !== true && (cb = await _txn._commit())
      this.head = log.head
      this.seq = seq
      cb()

    } catch (err) {
      // auto txn
      _txn !== true && await _txn.abort().catch(noop)
      await this._pruneEmpty().catch(noop)
      throw err
    }

    return first
  }

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
      _txn = await this.txn(seq, {})
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

      let sum = 0n
      let head = null
      for (const log of this.logs) {
        const rel = seq - sum
        await log.trim(rel)
        sum += log.seq + 1n
        head = log.head ?? head
      }

      this.rollForwardCb(seq)
      this.head = head
      this.seq = seq

      if (txn) { return _txn }
      await _txn.commit()

    } catch (err) {
      if (!txn) { await _txn.abort().catch(noop) }
      throw err
    } finally {
      await this._pruneEmpty().catch(noop)
    }
  }

  iter(seq=0n, opts={}) {
    const { path } = this
    if (!opts.txn && (!this._open || this._closing)) { throw new Error(`${path} is not open`) }
    if (typeof seq !== 'bigint') { throw new Error(`${path} seq must be big int`) }
    if (seq < 0n) { throw new Error(`${path} seq must be >= 0`) }
    const iter = new MultiIterator(this, seq, opts)
    this.iterators = this.iterators.filter((iter) => iter._open)
    this.iterators.push(iter)
    return opts.clazz ? iter : iter.lazy()
  }

  async del() {
    const { path } = this
    if (this._open) { throw new Error(`${path} is open`) }
    let logs = await this.logsFn(this)
    logs = await Promise.all(logs)
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
    this.path = `${log.path}-${opts.name}`
    if (opts.iterStepSize <= 0) { throw new Error(`${this.path} step size must be > 0`) }
    this.iterStepSize = opts.iterStepSize
    this.last = log.seq
    this.log = log
    this.seq = seq
    this.opts = opts
    this.txn = opts.txn
    this._open = true
    this.plan()
  }

  plan() {
    const { seq: next, iterStepSize } = this
    let sum = 0n
    this.iters = []
    for (const log of this.log.logs) {
      const rel = max(0n, next - sum)
      if (rel <= log.seq) {
        const iter = log.iter(rel, { iterStepSize, clazz: true })
        this.iters.push(iter)
      }
      sum += log.seq + 1n
    }
  }

  close() {
    this.iters.forEach((iter) => iter.close())
    this.iters = []
    this._open = false
    if (this.opts.txn) { return }
    this.txn && this.txn.commit().catch(noop)
  }

  async *lazy() {
    const { path, log } = this
    const { iters, last } = this
    let next = this.seq

    try {

      if (!this._open) { return }
      this.txn = this.txn ?? await log.txn(null)

      while (iters.length > 0 && next <= last) {
        if (!this._open) { return }
        for (const iter of iters) {
          if (!this._open) { return }
          for await (const data of iter.lazy()) {
            if (!this._open) { return }
            yield data
            next++
          }
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

module.exports = { MultiFsLog }
