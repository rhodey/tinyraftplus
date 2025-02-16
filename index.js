const crypto = require('crypto')
const TinyRaft = require('tinyraft')
const { FsLog } = require('./lib/fslog.js')
const { ConcurrentLog, AutoRestartLog } = require('./lib/others.js')
const { TcpLogServer, TcpLogClient } = require('./lib/remote.js')
const { Encoder, XxHashEncoder, EncryptingEncoder } = require('./lib/encoders.js')

const ACK = 'ack'
const APPEND = 'append'
const FORWARD = 'forward'
const FOLLOWER = 'follower'
const LEADER = 'leader'

const noop = () => {}

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

function awaitResolve(promises, minimum) {
  return new Promise((res, rej) => {
    let c = 0
    promises.forEach((promise) => {
      promise.then(() => {
        if (++c < minimum) { return }
        res()
      }).catch(noop)
    })
  })
}

function awaitChange(node, fn) {
  return new Promise((res, rej) => {
    const listen = (state) => {
      if (!fn(state)) { return }
      node.removeListener('change', listen)
      res()
    }
    node.on('change', listen)
  })
}

const defaults = {
  heartbeatTimeout: 500,
  electionTimeout: 2_500,
  leadershipTimeout: 5_000, // todo: ?? should be < electionTimeout
  initialTerm: 0, leaderPriority: 0,
  followerAckTimeout: 2_500,
  leaderAckTimeout: 5_000,
  logTimeout: 1_500,
  crypto: null,
}

// todo: allow start, stop, start
class RaftNode extends TinyRaft {
  constructor(nodeId, nodes, send, log, opts={}) {
    opts = { ...opts, nodeId, nodes, send }
    opts = { ...defaults, ...opts }
    super(opts)
    this.setMaxListeners(1024)
    const minFollowers = Math.ceil((nodes.length - 1) / 2)
    this.minFollowers = opts.minFollowers ? opts.minFollowers : minFollowers
    this.followerAckTimeout = opts.followerAckTimeout
    this.leaderAckTimeout = opts.leaderAckTimeout
    this.crypto = opts.crypto
    this._stopped = false
    this._prev = Promise.resolve(1)
    this.log = log
    this.seq = -1n
    this.head = null
  }

  open() {
    return this.log.open()
  }

  async _decryptHead(head=null) {
    if (!this.crypto) {
      this.head = this.log.head
      this.seq = this.log.seq
    } else if (head === null) {
      head = this.log.head
    }
    const ok = await this.crypto.decode(this.log, head)
    this.head = ok.body
    this.seq = ok.seq
    // todo: validate prev
  }

  async start() {
    const { nodeId: name } = this
    if (this._stopped) { throw new Error(`${name} raft node is stopped`) }
    super.start()
    await this.log.start()
      .then(() => this._decryptHead())
      .catch((err) => this.emit('error', err))
  }

  async stop() {
    this._stopped = true
    super.stop()
    await this.log.stop()
      .catch((err) => this.emit('error', err))
  }

  isLeader(state) {
    const followers = this.followers ?? []
    const have = followers.length - 1
    if (have < this.minFollowers) { return false }
    return state ? state.state === LEADER : this.state === LEADER
  }

  async awaitLeader() {
    const { nodeId: name } = this
    if (this._stopped) { throw new Error(`${name} raft node is stopped`) }
    const leading = (state) => this.isLeader(state)
    const following = (state) => state.state === FOLLOWER && state.leader !== null
    const fn = (state) => leading(state) || following(state)
    if (fn(this)) { return }
    return awaitChange(this, fn)
  }

  setNodes(nodes) {
    super.setNodes(nodes)
    // todo: keep opts setting
    this.minFollowers = Math.ceil((nodes.length - 1) / 2)
  }

  markAlive() {
    if (this._stopped) { return }
    super.markAlive()
  }

  onReceive(from, msg) {
    if (this._stopped) { return }
    this.emit('receive', [from, msg])
    if (this.leader === from) { this.markAlive() }
    const ack = { type: ACK, cid: msg.cid }
    switch (msg.type) {
      case FORWARD:
        if (!this.isLeader()) { return }
        if (!this.followers.includes(from)) { return }
        if (this.term !== msg.term) { return }
        return this._appendToSelfAndFollowers(msg.data, msg.nonces)
          .then((ok) => this.send(from, { ...ack, seq: ok.seq }))
          .catch((err) => this.emit('error', err))

      case APPEND:
        if (this.leader !== from) { return }
        if (this.term !== msg.term) { return }
        const { data, seq } = msg
        const work = Array.isArray(data) ? this.log.appendBatch(data, seq) : this.log.append(data, seq)
        const head = Array.isArray(data) ? data[data.length-1] : data
        return work.then(() => this._decryptHead(head))
          .then(() => this.send(from, ack))
          .catch((err) => this.emit('error', err))
    }
    super.onReceive(from, msg)
  }

  _awaitAck(from, cid) {
    return new Promise((res, rej) => {
      const listen = (arr) => {
        const [fromm, msg] = arr
        if (cid !== msg.cid) { return }
        if (ACK !== msg.type || from !== fromm) { return }
        this.removeListener('receive', listen)
        res(msg)
      }
      this.on('receive', listen)
    })
  }

  _fwdToLeader(data) {
    const { nodeId: name } = this
    const [timer, timedout] = timeout(this.leaderAckTimeout)
    const work = new Promise((res, rej) => {
      timedout.catch((err) => rej(new Error(`${name} forward to leader timeout`)))
      const nonce = () => {
        if (!this.crypto) { return }
        if (!Array.isArray(data)) { return this.crypto.nonce() }
        return this.crypto.nonce(data.length)
      }
      this.awaitLeader().then(() => {
        const cid = crypto.randomUUID()
        const nonces = nonce()
        const msg = { type: FORWARD, term: this.term, cid, data, nonces }
        const ack = this._awaitAck(this.leader, cid)
        this.send(this.leader, msg)
        return ack
      }).then(res).catch(rej)
    })
    work.catch(noop).finally(() => clearTimeout(timer))
    return work
  }

  _appendToFollowers(data, seq) {
    const { nodeId: name } = this
    const [timer, timedout] = timeout(this.followerAckTimeout)
    const work = new Promise((res, rej) => {
      timedout.catch((err) => rej(new Error(`${name} append to followers timeout`)))
      const cid = crypto.randomUUID()
      const msg = { type: APPEND, term: this.term, cid, data, seq }
      const followers = this.followers ?? []
      const acks = followers.filter((id) => this.nodeId !== id).map((id) => {
        const ack = this._awaitAck(id, cid)
        this.send(id, msg)
        return ack
      })
      awaitResolve(acks, this.minFollowers).then(() => res({ seq })).catch(rej)
    })
    work.catch(noop).finally(() => clearTimeout(timer))
    return work
  }

  async _encrypt(data, seq, nonces) {
    if (!this.crypto) { return data }
    const batch = Array.isArray(data)
    data = batch ? data : [data]
    let prev = this.head
    const works = data.map((body, i) => {
      const nonce = nonces ? nonces.slice(i*24, (i+1)*24) : undefined
      const buf = this.crypto.encode(this.log, seq++, prev, body, nonce)
      prev = body
      return buf
    })
    const ready = await Promise.all(works)
    return batch ? ready : ready[0]
  }

  _appendToSelfAndFollowers(data, nonces=null) {
    this._prev = this._prev.catch(noop).then(async () => {
      const { nodeId: name } = this
      const need = this.minFollowers
      let have = this.followers?.length ?? 0
      if (--have < need) { throw new Error(`${name} append to self needs ${need} followers have ${have}`) }
      const seq = this.seq + 1n
      const ready = await this._encrypt(data, seq, nonces)
      const work = Array.isArray(ready) ? this.log.appendBatch(ready, seq) : this.log.append(ready, seq)
      return work.then((seq2) => {
        if (Array.isArray(data)) {
          this.seq = seq + BigInt(data.length-1)
          this.head = data[data.length-1]
        } else {
          this.seq = seq
          this.head = data
        }
        return { ready, seq }
      })
    })
    return this._prev.then((ok) => this._appendToFollowers(ok.ready, ok.seq))
  }

  async append(data) {
    const { nodeId: name } = this
    if (this._stopped) { throw new Error(`${name} raft node is stopped`) }
    const work = this.isLeader() ? this._appendToSelfAndFollowers(data) : this._fwdToLeader(data)
    return work.then((ok) => ok.seq)
  }

  async appendBatch(data) {
    const { nodeId: name } = this
    if (this._stopped) { throw new Error(`${name} raft node is stopped`) }
    const work = this.isLeader() ? this._appendToSelfAndFollowers(data) : this._fwdToLeader(data)
    return work.then((ok) => ok.seq)
  }
}

module.exports = {
  RaftNode, FsLog,
  ConcurrentLog, AutoRestartLog,
  TcpLogServer, TcpLogClient,
  Encoder, XxHashEncoder, EncryptingEncoder,
}
