const net = require('net')
const crypto = require('crypto')
const { EventEmitter } = require('events')
const { PackrStream, UnpackrStream } = require('msgpackr')

// todo: iters

const randID = () => crypto.randomUUID()

function tcpServer(port, errCb, closeCb, msgCb) {
  const server = net.createServer((sock) => {
    const pack = new PackrStream()
    sock.sid = pack.sid = randID()
    const unpack = new UnpackrStream()
    pack.on('error', (err) => errCb(sock, new Error(`${port} pack error ${err.message}`)))
    unpack.on('error', (err) => errCb(sock, new Error(`${port} unpack error ${err.message}`)))
    sock.on('error', (err) => errCb(sock, new Error(`${port} sock error ${err.message}`)))
    sock.once('close', () => { pack.destroy(); unpack.destroy(); closeCb(sock) })
    pack.once('close', () => sock.destroy())
    pack.pipe(sock)
    sock.pipe(unpack).on('data', (msg) => msgCb(pack, msg))
  })
  return new Promise((res, rej) => {
    try {
      server.listen(port, '0.0.0.0', () => res(server))
    } catch (err) {
      rej(err)
    }
  })
}

class TcpLogServer extends EventEmitter {
  constructor(port, logFn) {
    super()
    this.port = port
    this.logFn = logFn
    this.server = null
    this.logs = {}
  }

  open() {
    return this.server !== null
  }

  async _logFor(msg) {
    const path = msg?.path
    const log = this.logs[path]
    if (!log) { throw new Error(`log ${path} not available`) }
    return log
  }

  _disconnect(sock) {
    const works = Object.keys(this.logs).map((path) => {
      const log = this.logs[path]
      log.clients.delete(sock.sid)
      if (log.clients.size > 0) { return null }
      delete this.logs[path]
      return log.stop()
    }).filter((ok) => ok)
    return Promise.all(works)
  }

  async _receive(sock, msg) {
    if (!this.open()) { return }
    switch (msg?.type) {
      case 'new':
        if (!msg.path) { throw new Error(`log path ${msg.path} not valid`) }
        if (!msg.args) { throw new Error(`log args not valid`) }
        let log = this.logs[msg.path]
        if (!log) { log = this.logs[msg.path] = this.logFn(msg.args) }
        log.clients = log.clients ?? new Set()
        log.clients.add(sock.sid)
        return sock.write({ type: 'ack', cid: msg.cid })

      case 'start':
        return this._logFor(msg).then((log) => {
          return log.start()
            .then(() => sock.write({ type: 'ack', cid: msg.cid, seq: log.seq, head: log.head }))
        })

      case 'stop':
        return this._logFor(msg)
          .then((log) => log.stop())
          .then(() => sock.write({ type: 'ack', cid: msg.cid }))

      case 'append':
        return this._logFor(msg)
          .then((log) => log.append(msg.data, msg.seq))
          .then((seq) => sock.write({ type: 'ack', cid: msg.cid, seq }))

      case 'appendBatch':
        return this._logFor(msg)
          .then((log) => log.appendBatch(msg.data, msg.seq))
          .then((seq) => sock.write({ type: 'ack', cid: msg.cid, seq }))

      case 'truncate':
        return this._logFor(msg).then((log) => {
          return log.truncate(msg.seq)
            .then(() => sock.write({ type: 'ack', cid: msg.cid, head: log.head }))
        })

      case 'del':
        return this._logFor(msg)
          .then((log) => log.del())
          .then(() => sock.write({ type: 'ack', cid: msg.cid }))

      case 'iter':
      default:
        sock.destroy()
    }
  }

  async start() {
    if (this.server) { return }
    const clientErr = (sock, err) => {
      this.emit('clientError', err)
      sock.destroy()
    }
    const clientClose = (sock) => {
      this._disconnect(sock)
        .catch((err) => this.emit('error', new Error(`stop logs on disconnect error ${err.message}`)))
    }
    const msgCb = (sock, msg) => {
      this._receive(sock, msg)
        .then(() => this.update())
        .catch((err) => sock.write({ type: 'ack', cid: msg.cid, err: err.message }))
        .catch((err) => sock.destroy())
    }
    this.server = await tcpServer(this.port, clientErr, clientClose, msgCb)
    this.server.on('error', (err) => {
      this.emit('error', err)
      if (!this.server) { return }
      this.server.close()
      this.server = null
    })
    this.server.once('close', () => {
      this.server = null
      this.emit('close')
    })
  }

  async stop() {
    if (!this.server) { return }
    this.server.close()
    this.server = null
  }
}

function tcpClient(host, port, name, errCb, msgCb) {
  const sock = new net.Socket()
  return new Promise((res, rej) => {
    const pack = new PackrStream()
    const unpack = new UnpackrStream()
    pack.on('error', (err) => errCb(sock, new Error(`${name} pack error ${err.message}`)))
    unpack.on('error', (err) => errCb(sock, new Error(`${name} unpack error ${err.message}`)))
    sock.on('error', (err) => errCb(sock, new Error(`${name} sock error ${err.message}`)))
    sock.once('close', () => { pack.destroy(); unpack.destroy() })
    pack.once('close', () => sock.destroy())
    sock.once('connect', () => {
      pack.pipe(sock)
      sock.pipe(unpack).on('data', (msg) => msgCb(pack, msg))
      res(pack)
    })
    sock.connect(port, host)
  })
}

class TcpLogClient extends EventEmitter {
  constructor(host, port, path, logFn) {
    super()
    this.host = host
    this.port = port
    this.path = path
    this.name = `${host}:${port}${path}`
    this.logFn = logFn
    this.sock = null
    this._open = false
    this.cbs = {}
    this.seq = -1n
    this.head = null
  }

  open() {
    return this._open
  }

  _receive(sock, msg) {
    const cbs = this.cbs[msg.cid]
    if (!cbs) { return }
    const [res, rej] = cbs
    delete this.cbs[msg.cid]
    if (!this.sock) { rej(new Error(`${this.name} is not connected`)) }
    if (msg.err) { rej(new Error(msg.err)) }
    res(msg)
  }

  _send(cmd) {
    const okSock = ['new', 'start', 'del']
    return new Promise((res, rej) => {
      if (!this.sock && okSock.includes(cmd.type)) { return rej(new Error(`${this.name} is not connected`)) }
      if (!this.open() && !okSock.includes(cmd.type)) { return rej(new Error(`${this.name} is not open`)) }
      cmd.cid = randID()
      this.cbs[cmd.cid] = [res, rej]
      cmd.path = this.path
      this.sock.write(cmd)
    })
  }

  async _start() {
    if (this.sock) { return }
    const errCb = (sock, err) => {
      this.sock = null
      this._open = false
      this.emit('error', err)
      sock.destroy()
    }
    const msgCb = (sock, msg) => this._receive(sock, msg)
    this.sock = await tcpClient(this.host, this.port, this.name, errCb, msgCb)
    this.sock.once('close', () => {
      if (this.sock) { errCb(this.sock, new Error(`${this.name} connection closed`)) }
      this._open = false
      this.emit('close')
    })
  }

  async start() {
    await this._start()
    await this._send({ type: 'new', args: this.logFn() })
    await this._send({ type: 'start' }).then((msg) => {
      this.seq = msg.seq
      this.head = msg.head
      this._open = true
    })
  }

  async stop() {
    if (!this.sock) { return }
    const sock = this.sock
    this.sock = null
    this._open = false
    sock.destroy()
  }

  append(data, seq) {
    return this._send({ type: 'append', data, seq }).then((msg) => {
      this.seq = msg.seq
      this.head = data
      return msg.seq
    })
  }

  appendBatch(data, seq) {
    return this._send({ type: 'appendBatch', data, seq }).then((msg) => {
      this.seq = msg.seq + BigInt(data.length-1)
      this.head = data[data.length-1]
      return msg.seq
    })
  }

  truncate(seq=-1n) {
    return this._send({ type: 'truncate', seq }).then((msg) => {
      this.seq = seq
      this.head = msg.head
    })
  }

  iter(seq=0n, opts={}) {
    throw new Error('not impl')
  }

  async del() {
    await this._start()
    await this._send({ type: 'new', args: this.logFn() })
    await this._send({ type: 'del' }).then(() => {
      this.seq = -1n
      this.head = null
    })
  }
}

module.exports = { TcpLogServer, TcpLogClient }
