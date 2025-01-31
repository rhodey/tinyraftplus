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
  return fsp.open(path, flag).catch((err) => {
    if (err.code === 'ENOENT' && again) {
      return fsp.writeFile(path, Buffer.alloc(0))
        .then(() => openOrCreate(path, flag, false))
        .then((fh) => {
          fh.newFile = true
          return fh
        })
    } else if (err.code === 'ENOENT') {
      throw new Error(`file create error ${path}`)
    }
    throw new Error(`file open error ${path} - ${err.message}`)
  })
}

const sync = async (arg, name) => {
  const path = typeof arg === 'string' ? arg : null
  const fh = typeof arg === 'string' ? null : arg
  name = name ? name : path

  if (fh) {
    return fh.sync().catch((err) => {throw new Error(`sync fh error ${name} - ${err.message}`)})
  }

  try {
    const fd = fs.openSync(path, 'r')
    fs.fsyncSync(fd)
  } catch (err) {
    throw new Error(`sync fs error ${name} - ${err.message}`)
  }
}

const truncate = (fh, name, offset) => fh.truncate(offset).catch((err) => {throw new Error(`truncate ${name} error - ${err.message}`)})

const del = (path) => fsp.rm(path, {force: true}).catch((err) => {throw new Error(`del error ${path} - ${err.message}`)})

const closeOrNull = async (fh, path) => {
  if (fh === null) { return null }
  await fh.close().catch((err) => {
    if (err.code === 'ENOENT') { return null }
    throw new Error(`close error ${path} - ${err.message}`)
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
  if (theirs !== ours) { throw new Error(`hash of head ${log.seq} does not match append data.prev`) }
}

const enforceChainArr = (log, arr) => {
  enforceChain(log, arr[0])
  for (let i = 1; i < arr.length; i++) {
    const head = arr[i - 1]
    const seq = (BigInt(log.seq) + BigInt(i)).toString()
    enforceChain({ head, seq }, arr[i])
  }
}

// todo: use timeouts
// todo: flush dir for new files
// todo: no text in error messages
const defaults = {
  logTimeout: 1_500,
}

class FsLog {
  constructor(dir, name, opts={}) {
    opts = { ...defaults, ...opts }
    this.logTimeout = opts.logTimeout
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

  async _readOffset(fh, name) {
    const buf = await fh.readFile('utf8').catch((err) => {throw new Error(`read ${name} error - ${err.message}`)})
    const str = buf.toString('utf8')
    if (!str) { return null }
    return parseStrInt(str, new Error(`read ${name} parse error - ${str}`))
  }

  async _writeOffset(fh, name, offset) {
    const buf = offset === null ? null : Buffer.from(offset + '', 'utf8')
    const write = async () => {
      const { bytesWritten } = await fh.write(buf, 0, buf.byteLength, 0).catch((err) => {throw new Error(`write ${name} error - ${err.message}`)})
      if (bytesWritten !== buf.byteLength) { throw new Error(`write ${name} len error`) }
    }
    return buf ? write() : truncate(fh, name, 0)
  }

  async _writeLog(data) {
    const { fhlog: fh } = this
    data = JSON.stringify(data)
    const buf = Buffer.from(data, 'utf8')
    const { bytesWritten } = await fh.write(buf).catch((err) => {throw new Error(`write log error - ${err.message}`)})
    if (bytesWritten !== buf.byteLength) { throw new Error(`write log len error`) }
    return bytesWritten
  }

  async _readHead(offset) {
    const { fhlog: fh } = this
    const { size } = await fh.stat().catch((err) => {throw new Error(`read head stat error - ${err.message}`)})
    const len = size - offset
    const buf = Buffer.alloc(len)
    const { bytesRead } = await fh.read(buf, 0, len, offset).catch((err) => {throw new Error(`read head error - ${err.message}`)})
    if (bytesRead !== len) { throw new Error('read head len error') }
    const str = buf.toString('utf8')
    const less = str.substring(0, 32)
    const obj = parseJson(str, new Error(`read head parse error - ${less}`))
    return { len, obj }
  }

  async _handleLock() {
    const handles = []

    try {

      // nothing to do
      const fhlock = await openOrCreate(`${this.path}.lock`, 'r+')
      handles.push([fhlock, 'lock'])
      const offset = await this._readOffset(fhlock, 'lock')
      if (offset === null) { return }
      console.log('NOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO')
      console.log('NOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO')
      console.log('NOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO')
      console.log('NOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO')
      console.log('NOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO')
      throw new Error(`todo: handle lock - ${offset}`)

      // open log and offset files
      let works = []
      works.puth(openOrCreate(`${this.path}.log`, 'w'))
      works.puth(openOrCreate(`${this.path}.off`, 'w'))
      const [fhlog, fhoff] = await Promise.all(works)
      handles.push([fhlog, 'log'])
      handles.push([fhoff, 'off'])

      // truncate log
      works = []
      works.push(truncate(fhlog, 'log', 123))
      // restore offset
      works.push(this._writeOffset(fhoff, 'log', 123))
      await Promise.all(works)
      // sync both
      await Promise.all([sync(fhlog, 'log'), sync(fhoff, 'off')])

      // reset lock
      await this._writeOffset(fhlock, 'lock', null).then(() => sync(fhlock, 'lock'))

    } finally {
      await Promise.all(handles.map((arr) => closeOrNull(arr[0], arr[1])))
    }
  }

  async start() {
    if (this._open) { return }
    await this._handleLock()

    const works = []
    works.push(openOrCreate(`${this.path}.log`, 'a+').then((fh) => this.fhlog = fh))
    works.push(openOrCreate(`${this.path}.off`, 'r+').then((fh) => this.fhoff = fh))
    works.push(openOrCreate(`${this.path}.lock`, 'r+').then((fh) => this.fhlock = fh))

    await Promise.all(works).then(() => this._readOffset(this.fhoff, 'off')).then((offset) => {
      if (offset === null || offset === -1) {
        this.offset = -1
        this.head = null
        this.hlen = null
        this.seq = '-1'
        this._open = true
        return
      }
      return this._readHead(offset).then((ok) => {
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
    works.push(closeOrNull(this.fhlog, `${this.path}.log`).then(() => this.fhlog = null))
    works.push(closeOrNull(this.fhoff, `${this.path}.off`).then(() => this.fhoff = null))
    works.push(closeOrNull(this.fhlock, `${this.path}.lock`).then(() => this.fhlock = null))
    return Promise.all(works).then(() => {
      this.offset = this.head = this.hlen = this.seq = null
      this._open = false
    })
  }

  async append(data, seq=null) {
    let next = BigInt(this.seq) + 1n
    seq = seq !== null ? seq : next
    seq = parseBigInt(seq, new Error('seq must be string number'))
    if (next !== seq) { throw new Error(`log append next ${next} !== seq ${seq}`) }
    if (!isObj(data)) { throw new Error('data must be object') }
    if (!this._open) { throw new Error('log is not open') }
    enforceChain(this, data)
    data.seq = seq = seq.toString()
    data = sortObj(data)

    // write work to lock
    next = this.offset + this.hlen
    const offset = this.offset < 0 ? 0 : next
    await this._writeOffset(this.fhlock, offset).then(() => sync(this.fhlock, 'lock'))

    // write work
    let works = []
    works.push(this._writeLog(data))
    works.push(this._writeOffset(this.fhoff, offset))
    const [len, _] = await Promise.all(works)

    // flush work
    works = []
    works.push(sync(this.fhlog, 'log'))
    works.push(sync(this.fhoff, 'off'))
    await Promise.all(works)

    // reset lock
    await this._writeOffset(this.fhlock, null).then(() => sync(this.fhlock, 'lock'))

    // ok
    this.offset = offset
    this.head = data
    this.hlen = len
    this.seq = seq
    return { data, seq }
  }

  async appendBatch(data, seq=null) {
    const next = BigInt(this.seq) + 1n
    seq = seq !== null ? seq : next
    seq = parseBigInt(seq, new Error('seq must be string number'))
    if (next !== seq) { throw new Error(`log append batch next ${next} !== seq ${seq}`) }
    if (!Array.isArray(data)) { throw new Error('data must be array') }
    if (data.length <= 0) { throw new Error('data must be array with length >= 1') }
    if (!this._open) { throw new Error('log is not open') }
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
    if (this._open) { throw new Error('log is open') }
    const works = [`${this.path}.log`, `${this.path}.off`, `${this.path}.lock`]
    return Promise.all(works.map(del))
  }
}

module.exports = {
  FsLog
}
