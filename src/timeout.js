const noop = () => {}

const terr = new Error('timeout')

// use less timers by group 100ms
const timeout = (ms) => {
  let timer = null
  const timedout = new Promise((res, rej) => {
    const now = Date.now()
    let next = now + ms
    next = next - (next % 100)
    next = (100 + next) - now
    timer = setTimeout(rej, next, terr)
  })
  return [timer, timedout]
}

// timeouts in ms
const defaults = {
  default: 1_500,
  open: undefined,
  close: undefined,
  txn: undefined,
  commit: undefined,
  abort: undefined,
  append: undefined,
  appendBatch: undefined,
  lock: undefined,
  trim: undefined,
  iter: undefined,
  del: undefined,
}

// wrap a log with timeouts
class TimeoutLog {
  constructor(log, opts={}) {
    opts = { ...defaults, ...opts }
    this._openMs = opts.open ?? opts.default
    this._closeMs = opts.close ?? opts.default
    this._txnMs = opts.txn ?? opts.default
    this._commitMs = opts.commit ?? this._txnMs
    this._abortMs = opts.abort ?? this._txnMs
    this._appendMs = opts.append ?? opts.default
    this._appendBatchMs = opts.appendBatch ?? opts.default
    this._lockMs = opts.lock ?? this._txnMs
    this._trimMs = opts.trim ?? opts.default
    this._iterMs = opts.iter ?? opts.default
    this._delMs = opts.del ?? opts.default
    this.log = log
    this.name = log.name
    this.path = log.path
    this.seq = null
    this.head = null
    this.iterators = []
  }

  get isOpen() {
    return this.log.isOpen
  }

  _sync(ok) {
    this.seq = this.log.seq
    this.head = this.log.head
    return ok
  }

  open() {
    const { path } = this
    const [timer, timedout] = timeout(this._openMs)
    const work = new Promise((res, rej) => {
      timedout.catch((err) => rej(new Error(`${path} open timeout`)))
      this.log.open().then(res).catch(rej)
    })
    work.catch(noop).finally(() => clearTimeout(timer))
    return work.then(() => this._sync()).catch((err) => {
      this._sync()
      return Promise.reject(err)
    })
  }

  close() {
    const { path } = this
    this.iterators.forEach((iter) => iter.close())
    this.iterators = []
    const [timer, timedout] = timeout(this._closeMs)
    const work = new Promise((res, rej) => {
      timedout.catch((err) => rej(new Error(`${path} close timeout`)))
      this.log.close().then(res).catch(rej)
    })
    work.catch(noop).finally(() => clearTimeout(timer))
    return work.then(() => this._sync()).catch((err) => {
      this._sync()
      return Promise.reject(err)
    })
  }

  _wrapTxn(txn) {
    const { path } = this
    const append = (data, seq) => {
      let [timer, timedout] = timeout(this._appendMs)
      timedout = timedout.catch(() => Promise.reject(new Error(`${path} txn append timeout`)))
      let work = txn.append(data, seq)
      work = Promise.race([timedout, work])
      work.catch(noop).finally(() => clearTimeout(timer))
      return work.then((ok) => this._sync(ok)).catch((err) => {
        this._sync()
        return Promise.reject(err)
      })
    }
    const appendBatch = (data, seq) => {
      let [timer, timedout] = timeout(this._appendBatchMs)
      timedout = timedout.catch(() => Promise.reject(new Error(`${path} txn appendBatch timeout`)))
      let work = txn.appendBatch(data, seq)
      work = Promise.race([timedout, work])
      work.catch(noop).finally(() => clearTimeout(timer))
      return work.then((ok) => this._sync(ok)).catch((err) => {
        this._sync()
        return Promise.reject(err)
      })
    }
    const lock = (seq) => {
      let [timer, timedout] = timeout(this._lockMs)
      timedout = timedout.catch(() => Promise.reject(new Error(`${path} txn lock timeout`)))
      let work = txn.lock(seq)
      work = Promise.race([timedout, work])
      work.catch(noop).finally(() => clearTimeout(timer))
      return work.then((ok) => this._sync(ok)).catch((err) => {
        this._sync()
        return Promise.reject(err)
      })
    }
    const commit = () => {
      let [timer, timedout] = timeout(this._commitMs)
      timedout = timedout.catch(() => Promise.reject(new Error(`${path} txn commit timeout`)))
      let work = txn.commit()
      work = Promise.race([timedout, work])
      work.catch(noop).finally(() => clearTimeout(timer))
      return work.then((ok) => this._sync(ok)).catch((err) => {
        this._sync()
        return Promise.reject(err)
      })
    }
    const abort = () => {
      let [timer, timedout] = timeout(this._abortMs)
      timedout = timedout.catch(() => Promise.reject(new Error(`${path} txn abort timeout`)))
      let work = txn.abort()
      work = Promise.race([timedout, work])
      work.catch(noop).finally(() => clearTimeout(timer))
      return work.then((ok) => this._sync(ok)).catch((err) => {
        this._sync()
        return Promise.reject(err)
      })
    }
    return { append, appendBatch, lock, commit, abort }
  }

  txn(seq=undefined) {
    const { path } = this
    const [timer, timedout] = timeout(this._txnMs)
    const work = new Promise((res, rej) => {
      timedout.catch((err) => rej(new Error(`${path} txn timeout`)))
      this.log.txn(seq).then((txn) => {
        const wrap = this._wrapTxn(txn)
        res(wrap)
      }).catch(rej)
    })
    work.catch(noop).finally(() => clearTimeout(timer))
    return work.then((ok) => this._sync(ok)).catch((err) => {
      this._sync()
      return Promise.reject(err)
    })
  }

  append(data, seq=null) {
    const { path } = this
    const [timer, timedout] = timeout(this._appendMs)
    const work = new Promise((res, rej) => {
      timedout.catch((err) => rej(new Error(`${path} append timeout`)))
      this.log.append(data, seq).then(res).catch(rej)
    })
    work.catch(noop).finally(() => clearTimeout(timer))
    return work.then((ok) => this._sync(ok)).catch((err) => {
      this._sync()
      return Promise.reject(err)
    })
  }

  appendBatch(data, seq=null) {
    const { path } = this
    const [timer, timedout] = timeout(this._appendBatchMs)
    const work = new Promise((res, rej) => {
      timedout.catch((err) => rej(new Error(`${path} appendBatch timeout`)))
      this.log.appendBatch(data, seq).then(res).catch(rej)
    })
    work.catch(noop).finally(() => clearTimeout(timer))
    return work.then((ok) => this._sync(ok)).catch((err) => {
      this._sync()
      return Promise.reject(err)
    })
  }

  async trim(seq=-1n, txn=false) {
    const { path } = this
    if (typeof seq !== 'bigint') { throw new Error(`${path} seq must be big int`) }
    if (seq < -1n) { throw new Error(`${path} seq must be >= -1`) }
    this.iterators.filter((iter) => iter.last > seq).forEach((iter) => iter.close())
    this.iterators = this.iterators.filter((iter) => iter._open)
    const [timer, timedout] = timeout(this._trimMs)
    const work = new Promise((res, rej) => {
      timedout.catch((err) => rej(new Error(`${path} trim timeout`)))
      this.log.trim(seq, txn).then(res).catch(rej)
    })
    work.catch(noop).finally(() => clearTimeout(timer))
    return work.then((ok) => this._sync(ok)).catch((err) => {
      this._sync()
      return Promise.reject(err)
    }).then((ok) => {
      if (!txn) { return ok }
      return this._wrapTxn(ok)
    })
  }

  iter(seq=0n, opts={}) {
    const o = {...opts, clazz: true}
    const clazz = this.log.iter(seq, o)
    opts.iterMs = opts.iterMs ?? this._iterMs
    const iter = new TimeoutIterator(this, seq, clazz, opts)
    this.iterators = this.iterators.filter((iter) => iter._open)
    this.iterators.push(iter)
    return opts.clazz ? iter : iter.lazy()
  }

  del() {
    const { path } = this
    const [timer, timedout] = timeout(this._delMs)
    const work = new Promise((res, rej) => {
      timedout.catch((err) => rej(new Error(`${path} del timeout`)))
      this.log.del().then(res).catch(rej)
    })
    work.catch(noop).finally(() => clearTimeout(timer))
    return work.then((ok) => this._sync(ok)).catch((err) => {
      this._sync()
      return Promise.reject(err)
    })
  }
}

class TimeoutIterator {
  constructor(log, seq, clazz, opts) {
    opts = { name: 'iter', ...opts }
    this.path = `${log.path}-${opts.name}`
    this.iterMs = opts.iterMs
    this.seq = seq
    this.last = log.seq
    this._clazz = clazz
    this._open = true
  }

  close() {
    this._clazz.close()
    this._open = false
  }

  async *lazy() {
    let timer = null
    let timedout = null
    let next = this.seq
    try {

      if (!this._open) { return }
      const lazy = this._clazz.lazy()

      while (next <= this.last) {
        if (!this._open) { return }
        const arr = timeout(this.iterMs)
        timer = arr[0]; timedout = arr[1]
        timedout = timedout.catch(() => Promise.reject(new Error(`${this.path} iter timeout`)))
        let nextt = lazy.next()
        const work = Promise.race([timedout, nextt])

        nextt = await work
        clearTimeout(timer)
        if (!this._open) { return }
        if (nextt.done) { break }
        next++
        yield nextt.value
      }

    } finally {
      clearTimeout(timer)
      this.close()
    }
  }
}

module.exports = { TimeoutLog }
