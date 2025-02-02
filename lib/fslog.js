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
    // fh = fh ? fh : this.fhoff2
    const { size } = await fh.stat({bigint: true}).catch((err) => {throw new Error(`${fh.name} stat error - ${err.message}`)})
    console.log('READ OFF SIZE', size)
    if (size <= 0n) { return null }
    const buf = Buffer.alloc(16)
    const pos = size - BigInt(16)
    if (size < buf.byteLength) { throw new Error(`${fh.name} read size < buf len error`) }
    const { bytesRead } = await fh.read(buf, 0, 16, pos).catch((err) => {throw new Error(`${fh.name} read error - ${err.message}`)})
    if (bytesRead !== buf.byteLength) { throw new Error(`${fh.name} read len error`) }
    const seq = buf.readBigInt64BE(0)
    const off = buf.readBigInt64BE(8)
    console.log('READ OFF', seq, off)
    return { seq, offset: off }
  }

  async _appendOffset(arg1, arg2, arg3) {
    const fh = arg3 !== undefined ? arg1 : this.fhoff
    const seq = arg3 !== undefined ? arg2 : arg1
    const off = arg3 !== undefined ? arg3 : arg2
    const buf = Buffer.alloc(16)
    buf.writeBigInt64BE(seq)
    buf.writeBigInt64BE(off, 8)
    console.log('APPEND OFF', seq, off)
    const write = async () => {
      const { bytesWritten } = await fh.write(buf).catch((err) => {throw new Error(`${fh.name} write error - ${err.message}`)})
      if (bytesWritten !== buf.byteLength) { throw new Error(`${fh.name} write len error`) }
    }
    return write()
  }

  async _readLock(fh) {
    fh = fh ? fh : this.fhlock
    const buf = await fh.readFile().catch((err) => {throw new Error(`${fh.name} read error - ${err.message}`)})
    if (buf.byteLength !== 24) { throw new Error(`${fh.name} read len != 24 error`) }
    const seq = buf.readBigInt64BE(0)
    const hoff = buf.readBigInt64BE(8)
    const hlen = buf.readBigInt64BE(16)
    return { seq, hoff, hlen }
  }

  async _writeLock(arg1, arg2, arg3, arg4) {
    const fh = arg4 !== undefined ? arg1 : this.fhlock
    const seq = arg4 !== undefined ? arg2 : arg1
    const hoff = arg4 !== undefined ? arg3 : arg2
    const hlen = arg4 !== undefined ? arg4 : arg3
    if (seq === null) { return truncate(fh, 0) }
    const buf = Buffer.alloc(24)
    buf.writeBigInt64BE(seq)
    buf.writeBigInt64BE(hoff, 8)
    buf.writeBigInt64BE(hlen, 16)
    const write = async () => {
      const { bytesWritten } = await fh.write(buf, 0, buf.byteLength, 0).catch((err) => {throw new Error(`${fh.name} write error - ${err.message}`)})
      if (bytesWritten !== buf.byteLength) { throw new Error(`${fh.name} write len error`) }
    }
    return write()
  }

  async _appendLog(arg1, arg2) {
    const fh = arg2 !== undefined ? arg1 : this.fhlog
    const data = arg2 !== undefined ? arg2 : arg1
    const { bytesWritten } = await fh.write(data).catch((err) => {throw new Error(`${fh.name} write error - ${err.message}`)})
    if (bytesWritten !== data.byteLength) { throw new Error(`${fh.name} write len error`) }
    return bytesWritten
  }

  async _readHead(arg1, arg2) {
    const fh = arg2 !== undefined ? arg1 : this.fhlog
    const offset = arg2 !== undefined ? arg2 : arg1
    const { size } = await fh.stat({bigint: true}).catch((err) => {throw new Error(`${fh.name} stat error - ${err.message}`)})
    const len = Number(size - offset)
    if (len < 8) { throw new Error(`${fh.name} read len < 8 error`) }
    const buf = Buffer.alloc(len)
    const { bytesRead } = await fh.read(buf, 0, len, offset).catch((err) => {throw new Error(`${fh.name} read error - ${err.message}`)})
    if (bytesRead !== len) { throw new Error(`${fh.name} read len error`) }
    const seq = buf.readBigInt64BE(0)
    const head = buf.slice(8)
    return { seq, head }
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

      const open = async (path, flag) => {
        const fh = await openOrCreate(path, flag)
        handles.push(fh)
        return fh
      }

      // rollback
      let works = []
      works.push(open(`${this.path}.log`, 'r+'))
      works.push(open(`${this.path}.off`, 'w'))
      const [fhlog, fhoff] = await Promise.all(works)

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
    works.push(openOrCreate(`${this.path}.off`, 'a+').then((fh) => this.fhoff = fh))
    // works.push(openOrCreate(`${this.path}.off`, 'r').then((fh) => this.fhoff2 = fh))
    works.push(openOrCreate(`${this.path}.lock`, 'r+').then((fh) => this.fhlock = fh))

    await Promise.all(works).then((arr) => {
      const neww = arr.filter((fh) => fh.isNew)
      if (neww.length <= 0) { return }
      return Promise.all(neww.map(sync)).then(() => sync(this.dir))
    })

    return this._readOffset().then((ok) => {
      if (ok === null || ok.offset === -1n) {
        this.offset = -1n
        this.head = null
        this.hlen = -1n
        this.seq = '-1'
        this._open = true
        return
      }
      const { seq, offset } = ok
      console.log('OFFFFFFF', seq, offset)
      return this._readHead(offset).then((ok) => {
        this.offset = offset
        this.head = ok.head
        this.hlen = ok.head.byteLength
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
    if (!(data instanceof Buffer)) { throw new Error(`${name} data must be buffer`) }
    if (!this._open) { throw new Error(`${name} is not open`) }

    // state to lock
    await this._writeLock(seq, this.offset, this.hlen)
      .then(() => sync(this.fhlock))

    // work
    let works = []
    works.push(this._appendLog(data))
    next = this.offset + this.hlen
    const offset = this.offset < 0n ? 0n : next
    works.push(this._appendOffset(seq, offset))
    let [len, _] = await Promise.all(works)
    len = BigInt(len)

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
    seq = seq.toString()
    this.offset = offset
    this.head = data
    this.hlen = len
    this.seq = seq
    return { seq, data }
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
    // todo: fs
    this.log = this.log.concat(data)
    this.seq = seq
    this.head = this.log[this.seq]
    return { data, seq: next.toString() }
  }

  async del() {
    const { path: name } = this
    if (this._open) { throw new Error(`${name} is open`) }
    const works = [`${this.path}.log`, `${this.path}.off`, `${this.path}.lock`]
    return Promise.all(works.map(del)).then(() => sync(this.dir))
  }
}

module.exports = {
  FsLog
}
