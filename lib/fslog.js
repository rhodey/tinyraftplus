const fs = require('fs')
const fsp = require('fs/promises')
const crypto = require('crypto')

const noop = () => {}

const isObj = (data) => data && typeof data === 'object' && !Array.isArray(data)

// todo: use timeouts
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

const parseUInt = (str, err) => {
  const uint = parseInt(str)
  if (isNaN(uint) || uint < 0) { throw err }
  return uint
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

const openOrCreate = (path, again=true) => {
  return fsp.open(path, 'r+').catch((err) => {
    if (err.code === 'ENOENT' && again) {
      return fsp.writeFile(path, Buffer.alloc(0))
        .then(() => openOrCrete(path, false))
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

const closeOrNull = async (fh, path) => {
  if (fh === null) { return null }
  await fh.close().catch((err) => {
    if (err.code === 'ENOENT') { return null }
    throw new Error(`close error ${path} - ${err.message}`)
  })
  return true
}

// todo: flush dir for new files
const syncDir = async (path) => {
  try {
    const fd = fs.openSync(path, 'r')
    fs.fsyncSync(fd)
  } catch (err) {
    throw new Error(`dir fsync error ${path} - ${err.message}`)
  }
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
    this.seq = null
    this.log = null // todo: remove
  }

  open() {
    return this._open
  }

  async _readOff() {
    const { fhoff: fh } = this
    if (fh === null) { return null }
    let str = await fh.readFile('utf8').catch((err) => {throw new Error(`read off error - ${err.message}`)})
    str = str.substring(0, 32)
    if (!str) { return null }
    return parseUInt(str, new Error(`read off parse error - ${str}`))
  }

  async _readHead(offset) {
    const { fhlog: fh } = this
    if (fh === null) { return null }
    const { size } = await fh.stat().catch((err) => {throw new Error(`read head stat error - ${err.message}`)})
    const len = size - offset
    const buf = Buffer.alloc(len)
    const { bytesRead } = await fh.read(buf, 0, len, offset).catch((err) => {throw new Error(`read head error - ${err.message}`)})
    const str = buf.toString('utf8', 0, bytesRead)
    const less = str.substring(0, 32)
    return parseJson(str, new Error(`read head parse error - ${less}`))
  }

  async start() {
    if (this._open) { return }
    const works = []
    works.push(openOrCreate(`${this.path}.log`).then((fh) => this.fhlog = fh))
    works.push(openOrCreate(`${this.path}.off`).then((fh) => this.fhoff = fh))
    works.push(openOrCreate(`${this.path}.lock`).then((fh) => this.fhlock = fh))

    await Promise.all(works).then(() => this._readOff()).then((offset) => {
      this.offset = offset
      if (offset === null) {
        this.head = null
        this.seq = '-1'
        this._open = true
        this.log = [] // todo: remove
        return
      }
      return this._readHead(offset).then((head) => {
        this.head = head
        this.seq = head.seq
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
      this.offset = this.head = this.seq = null
      this._open = false
      this.log = null // todo: remove
    })
  }

  async append(data, seq=null) {
    const next = BigInt(this.seq) + 1n
    seq = seq !== null ? seq : next
    seq = parseBigInt(seq, new Error('seq must be string number'))
    if (next !== seq) { throw new Error(`log append next ${next} !== seq ${seq}`) }
    if (!isObj(data)) { throw new Error('data must be object') }
    if (!this._open) { throw new Error('log is not open') }
    enforceChain(this, data)
    data.seq = seq = seq.toString()
    data = sortObj(data)
    // todo: fs
    this.log.push(data)
    this.seq = seq
    this.head = this.log[seq]
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

  async remove(seq) {
    seq = parseBigInt(seq, new Error('seq must be string number'))
    if (seq < 0n) { throw new Error('seq must be >= 0') }
    if (!this._open) { throw new Error('log is not open') }
    this.seq = BigInt(this.seq)
    if (seq > this.seq) { return '0' }
    // todo: fs
    this.log = this.log.slice(0, parseInt(seq.toString()))
    const removed = (this.seq - seq) + 1n
    this.seq = (seq - 1n).toString()
    this.head = this.log[this.seq]
    return removed.toString()
  }
}

module.exports = {
  FsLog
}
