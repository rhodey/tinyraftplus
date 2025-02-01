const fs = require('fs')
const fsp = require('fs/promises')
const crypto = require('crypto')

const noop = () => {}

const isObj = (data) => data && typeof data === 'object' && !Array.isArray(data)

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

const parseStrInt = (str, err) => {
  const num = parseInt(str)
  if (isNaN(num)) { throw err }
  return num
}

const parseBigInt = (str, err) => {
  try {
    return BigInt(str)
  } catch (err2) {
    throw err
  }
}

const parseJson = (str, err) => {
  try {
    return JSON.parse(str)
  } catch (err2) {
    throw err
  }
}

const openOrCreate = (path, flag, again=true) => {
  return fsp.open(path, flag).then((fh) => {
    fh.name = path
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

const truncate = (fh, offset) => fh.truncate(offset).catch((err) => {throw new Error(`${fh.name} truncate error - ${err.message}`)})

const del = (path) => fsp.rm(path, {force: true}).catch((err) => {throw new Error(`${path} del error - ${err.message}`)})

const close = async (fh) => {
  if (fh === null) { return null }
  await fh.close().catch((err) => {
    if (err.code === 'ENOENT') { return null }
    throw new Error(`${fh.name} close error - ${err.message}`)
  })
  return true
}

const sortObj = (obj) => {
  if (obj === null) { return null }
  return Object.keys(obj).sort().reduce((acc, key) => {
    acc[key] = obj[key]
    return acc
  }, {})
}

const sha256 = (obj) => crypto.createHash('sha256').update(JSON.stringify(sortObj(obj))).digest('hex')

const enforceChain = (log, data) => {
  if (!data.prev) {
    data.prev = sha256(log.head)
    return
  }
  const theirs = data.prev
  const ours = sha256(log.head)
  if (theirs !== ours) { throw new Error(`${log.path} hash of head ${log.seq} does not match next.prev`) }
}

const enforceChainArr = (log, arr) => {
  enforceChain(log, arr[0])
  for (let i = 1; i < arr.length; i++) {
    const head = arr[i - 1]
    const seq = (BigInt(log.seq) + BigInt(i)).toString()
    enforceChain({ path: log.path, head, seq }, arr[i])
  }
}

// todo: fhlock flock
// todo: use timeouts
// todo: no file contents in error messages
const defaults = {
  logTimeout: 1_500,
  rollbackCb: noop,
}

class FsLog {
  constructor(dir, name, opts={}) {
    opts = { ...defaults, ...opts }
    this.logTimeout = opts.logTimeout
    this.rollbackCb = opts.rollbackCb
    this.dir = dir
    this.path = dir + name
    this._open = false
    this.fhlog = null
    this.fhoff = null
    this.fhlock = null
    this.offset = null
    this.head = null
    this.hlen = null
    this.seq = null
  }

  open() {
    return this._open
  }

  async _readOffset(fh) {
    fh = fh ? fh : this.fhoff
    console.log('READ OFF', fh.name)
    const buf = await fh.readFile('utf8').catch((err) => {throw new Error(`${fh.name} read error - ${err.message}`)})
    const str = buf.toString('utf8')
    if (!str) { return null }
    const less = str.substring(0, 32)
    return parseStrInt(less, new Error(`${fh.name} parse error - ${less}`))
  }

  async _writeOffset(arg1, arg2) {
    const fh = arg2 !== undefined ? arg1 : this.fhoff
    const offset = arg2 !== undefined ? arg2 : arg1
    const buf = Buffer.from(offset + '', 'utf8')
    console.log('WRITE OFF', fh.name, offset)
    const write = async () => {
      const { bytesWritten } = await fh.write(buf, 0, buf.byteLength, 0).catch((err) => {throw new Error(`${fh.name} write error - ${err.message}`)})
      if (bytesWritten !== buf.byteLength) { throw new Error(`${fh.name} write len error`) }
      console.log('WROTE OFF', fh.name, offset)
    }
    return write()
  }

  async _readLock(fh) {
    fh = fh ? fh : this.fhlock
    const buf = await fh.readFile('utf8').catch((err) => {throw new Error(`${fh.name} read error - ${err.message}`)})
    const str = buf.toString('utf8')
    console.log('READ LOCK', fh.name, str)
    if (!str) { return null }
    let [hoff, hlen] = str.split(':').map((str) => str.substring(0, 32))
    hoff = parseStrInt(hoff, new Error(`${fh.name} parse hoff error - ${hoff}`))
    hlen = parseStrInt(hlen, new Error(`${fh.name} parse hlen error - ${hlen}`))
    return { hoff, hlen }
  }

  async _writeLock(arg1, arg2) {
    const fh = arg2 !== undefined ? arg1 : this.fhlock
    const data = arg2 !== undefined ? arg2 : arg1
    const buf = data === null ? null : Buffer.from(data, 'utf8')
    console.log('WRITE LOCK', fh.name, data)
    const write = async () => {
      const { bytesWritten } = await fh.write(buf, 0, buf.byteLength, 0).catch((err) => {throw new Error(`${fh.name} write error - ${err.message}`)})
      if (bytesWritten !== buf.byteLength) { throw new Error(`${fh.name} write len error`) }
      console.log('WROTE LOCK', fh.name, data)
    }
    return buf ? write() : truncate(fh, 0).then(() => console.log('TRUNC LOCK', fh.name, data))
  }

  async _writeLog(data) {
    const { fhlog: fh } = this
    data = JSON.stringify(data)
    const buf = Buffer.from(data, 'utf8')
    const { bytesWritten } = await fh.write(buf).catch((err) => {throw new Error(`${fh.name} write error - ${err.message}`)})
    if (bytesWritten !== buf.byteLength) { throw new Error(`${fh.name} write len error`) }
    return bytesWritten
  }

  async _readHead(offset) {
    const { fhlog: fh } = this
    const { size } = await fh.stat().catch((err) => {throw new Error(`${fh.name} stat error - ${err.message}`)})
    const len = size - offset
    const buf = Buffer.alloc(len)
    const { bytesRead } = await fh.read(buf, 0, len, offset).catch((err) => {throw new Error(`${fh.name} read head error - ${err.message}`)})
    if (bytesRead !== len) { throw new Error(`${fh.name} read head len error`) }
    const str = buf.toString('utf8')
    const less = str.substring(0, 32)
    const obj = parseJson(str, new Error(`${fh.name} read head parse error - ${less}`))
    return { len, obj }
  }

  async _handleRollback() {
    const handles = []

    try {

      // check lock
      const fhlock = await openOrCreate(`${this.path}.lock`, 'r+')
      handles.push(fhlock)
      const ok = await this._readLock(fhlock)
      if (fhlock.isNew) {
        await sync(fhlock)
        await sync(this.dir)
      }

      // nothing to do
      if (ok === null) { return }

      // rollback
      let works = []
      works.push(openOrCreate(`${this.path}.log`, 'w'))
      works.push(openOrCreate(`${this.path}.off`, 'w'))
      const [fhlog, fhoff] = await Promise.all(works)
      handles.push(fhlog)
      handles.push(fhoff)

      // rollback
      works = []
      const { hoff, hlen } = ok
      const start = Math.max(hoff, 0)
      const end = Math.max(start + hlen, 0)
      works.push(truncate(fhlog, end))
      works.push(this._writeOffset(fhoff, hoff))
      await Promise.all(works)

      // rollback
      works = []
      works.push(sync(fhlog))
      works.push(sync(fhoff))
      await Promise.all(works)

      // reset lock
      await this._writeLock(fhlock, null)
        .then(() => sync(fhlock))

    } finally {
      await Promise.all(handles.map(close))
    }
  }

  async start() {
    if (this._open) { return }

    const works = []
    // await this._handleRollback()
    works.push(openOrCreate(`${this.path}.log`, 'a+').then((fh) => this.fhlog = fh))
    works.push(openOrCreate(`${this.path}.off`, 'r+').then((fh) => this.fhoff = fh))
    works.push(openOrCreate(`${this.path}.lock`, 'r+').then((fh) => this.fhlock = fh))

    await Promise.all(works).then((arr) => {
      const neww = arr.filter((fh) => fh.isNew)
      if (neww.length <= 0) { return }
      return Promise.all(neww.map(sync)).then(() => sync(this.dir))
    })

    return this._readOffset().then((offset) => {
      if (offset === null || offset === -1) {
        console.log('OFF NULL')
        this.offset = -1
        this.head = null
        this.hlen = -1
        this.seq = '-1'
        this._open = true
        return
      }
      return this._readHead(offset).then((ok) => {
        console.log('OFF OK')
        this.offset = offset
        this.head = ok.obj
        this.hlen = ok.len
        this.seq = ok.obj.seq
        this._open = true
      })
    })
  }

  stop() {
    const works = []
    works.push(close(this.fhlog, `${this.path}.log`).then(() => this.fhlog = null))
    works.push(close(this.fhoff, `${this.path}.off`).then(() => this.fhoff = null))
    works.push(close(this.fhlock, `${this.path}.lock`).then(() => this.fhlock = null))
    return Promise.all(works).then(() => {
      this.offset = this.head = this.hlen = this.seq = null
      this._open = false
    })
  }

  async append(data, seq=null) {
    const { path: name } = this
    let next = BigInt(this.seq) + 1n
    seq = seq !== null ? seq : next
    seq = parseBigInt(seq, new Error(`${name} seq must be string number`))
    if (next !== seq) { throw new Error(`${name} next ${next} !== ${seq}`) }
    if (!isObj(data)) { throw new Error(`${name} data must be object`) }
    if (!this._open) { throw new Error(`${name} is not open`) }
    enforceChain(this, data)
    data.seq = seq = seq.toString()
    data = sortObj(data)

    // state to lock
    const state = `${this.offset}:${this.hlen}`
    await this._writeLock(state)
      .then(() => sync(this.fhlock))

    // work
    let works = []
    works.push(this._writeLog(data))
    next = this.offset + this.hlen
    const offset = this.offset < 0 ? 0 : next
    works.push(this._writeOffset(offset))
    const [len, _] = await Promise.all(works)

    // flush work
    works = []
    works.push(sync(this.fhlog))
    works.push(sync(this.fhoff))
    await Promise.all(works)
    // for unit tests
    this.rollbackCb(seq)

    // reset lock
    await this._writeLock(null)
      .then(() => sync(this.fhlock))

    // ok
    this.offset = offset
    this.head = data
    this.hlen = len
    this.seq = seq
    return { data, seq }
  }

  async appendBatch(data, seq=null) {
    const { path: name } = this
    const next = BigInt(this.seq) + 1n
    seq = seq !== null ? seq : next
    seq = parseBigInt(seq, new Error(`${name} (batch) seq must be string number`))
    if (next !== seq) { throw new Error(`${name} (batch) next ${next} !== ${seq}`) }
    if (!Array.isArray(data)) { throw new Error(`${name} (batch) data must be array`) }
    if (data.length <= 0) { throw new Error(`${name} (batch) data must be array with length >= 1`) }
    if (!this._open) { throw new Error(`${name} (batch) is not open`) }
    data = data.map((elem, i) => {
      seq = (next + BigInt(i)).toString()
      elem.seq = elem.seq ? elem.seq : seq
      return elem
    })
    enforceChainArr(this, data)
    // todo: fs
    this.log = this.log.concat(data)
    this.seq = seq
    this.head = this.log[this.seq]
    return { data, seq: next.toString() }
  }

  async del() {
    if (this._open) { throw new Error(`${this.path} is open`) }
    const works = [`${this.path}.log`, `${this.path}.off`, `${this.path}.lock`]
    return Promise.all(works.map(del))
  }
}

module.exports = {
  FsLog
}
