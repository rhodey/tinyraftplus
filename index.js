const crypto = require('crypto')
const TinyRaft = require('tinyraft')
const { FsLog } = require('./lib/fslog.js')
const { ConcurrentLog, AutoRestartLog } = require('./lib/others.js')
const { Encoder, XxHashEncoder } = require('./lib/encoders.js')

const ACK = 'ack'
const APPEND = 'append'
const FORWARD = 'forward'
const FOLLOWER = 'follower'
const LEADER = 'leader'

const defaults = {
  heartbeatTimeout: 500,
  electionTimeout: 2_500,
  leadershipTimeout: 5_000, // todo: ?? should be < electionTimeout
  initialTerm: 0, leaderPriority: 0,
  followerAckTimeout: 2_500,
  leaderAckTimeout: 5_000,
  logTimeout: 1_500,
}

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

class RaftNode extends TinyRaft {
  constructor(nodeId, nodes, send, log, opts={}) {
    opts = { ...opts, nodeId, nodes, send }
    opts = { ...defaults, ...opts }
    super(opts)
    const minFollowers = Math.ceil((nodes.length - 1) / 2)
    this.minFollowers = opts.minFollowers ? opts.minFollowers : minFollowers
    this.followerAckTimeout = opts.followerAckTimeout
    this.leaderAckTimeout = opts.leaderAckTimeout
    this._stopped = false
    this.log = log
  }

  open() {
    return this.log.open()
  }

  async start() {
    const { nodeId: name } = this
    if (this._stopped) { throw new Error(`${name} raft node is stopped`) }
    super.start()
    return this.log.start()
  }

  stop() {
    this._stopped = true
    super.stop()
    return this.log.stop()
  }

  isLeader() {
    return this.state === LEADER
  }

  async awaitLeader() {
    const { nodeId: name } = this
    if (this._stopped) { throw new Error(`${name} raft node is stopped`) }
    const leading = (state) => state.state === LEADER
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
    super.emit('receive', [from, msg])
    if (this.leader === from) { this.markAlive() }
    const ack = { type: ACK, cid: msg.cid }
    switch (msg.type) {
      case FORWARD:
        // if (!this.state !== LEADER) { return }
        if (!this.followers.includes(from)) { return }
        if (this.term !== msg.term) { return }
        return this._appendToSelfAndFollowers(msg.data)
          .then((ok) => this.send(from, { ...ack, seq: ok.seq }))

      case APPEND:
        if (this.leader !== from) { return }
        if (this.term !== msg.term) { return }
        const { data, seq } = msg
        const work = Array.isArray(data) ?
          this.log.appendBatch(data, seq) : this.log.append(data, seq)
        return work.then(() => this.send(from, ack))
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
      this.awaitLeader().then(() => {
        const cid = crypto.randomUUID()
        const msg = { type: FORWARD, cid, data, term: this.term }
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
      const msg = { type: APPEND, cid, data, seq, term: this.term }
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

  async _appendToSelfAndFollowers(data) {
    const { nodeId: name } = this
    const need = this.minFollowers
    let have = this.followers?.length ?? 0
    if (--have < need) { throw new Error(`${name} append to self needs ${need} followers have ${have}`) }
    const work = Array.isArray(data) ? this.log.appendBatch(data) : this.log.append(data)
    return work.then((seq) => this._appendToFollowers(data, seq))
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
  Encoder, XxHashEncoder,
  ConcurrentLog, AutoRestartLog,
}
