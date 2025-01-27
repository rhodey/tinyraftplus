const crypto = require('crypto')
const TinyRaft = require('tinyraft')
const Decimal = require('decimal.js')

const LEADER = 'leader'
const FOLLOWER = 'follower'

const ACK = 'ack'
const APPEND = 'append'
const FORWARD = 'forward'

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

const isObj = (data) => data && typeof data === 'object' && !Array.isArray(data)

// round timers to nearest 100ms = use less timers
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

function awaitResolve(promises, count) {
  return new Promise((res, rej) => {
    let c = 0
    promises.forEach((promise) => {
      promise.then(() => {
        if (++c < count) { return }
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

class TinyRaftPlus extends TinyRaft {
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
    if (this._stopped) { throw new Error('raft node is stopped') }
    if (this.open()) { return super.start() } // tinyraft uses start more than once
    return this.log.start().then(() => super.start())
  }

  stop() {
    super.stop()
    this._stopped = true
    return this.log.stop()
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
        if (!this.followers.includes(from)) { return }
        if (this.term !== msg.term) { return }
        return this._appendToSelfAndFollowers(msg.data)
          .then((ok) => this.send(from, { ...ack, data: ok.data, seq: ok.seq }))

      case APPEND:
        if (this.leader !== from) { return }
        if (this.term !== msg.term) { return }
        const { data, seq } = msg
        const work = Array.isArray(data) ? this.log.appendBatch(data, seq) : this.log.append(data, seq)
        return work.then(() => this.send(from, ack))
    }

    super.onReceive(from, msg)
  }

  _awaitAck(from, cid) {
    return new Promise((res, rej) => {
      const listen = (arr) => {
        const [fromm, msg] = arr
        if (ACK !== msg.type || from !== fromm || cid !== msg.cid) { return }
        this.removeListener('receive', listen)
        res(msg)
      }
      this.on('receive', listen)
    })
  }

  async _awaitFollowing() {
    const following = (state) => state.state === FOLLOWER && state.leader !== null
    if (following(this)) { return }
    const fn = (state) => following(state)
    return awaitChange(this, fn)
  }

  _fwdToLeader(data) {
    const [timer, timedout] = timeout(this.leaderAckTimeout)
    const work = new Promise((res, rej) => {
      timedout.catch((err) => rej(new Error('forward to leader timeout')))
      this._awaitFollowing().then(() => {
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
    const [timer, timedout] = timeout(this.followerAckTimeout)
    const work = new Promise((res, rej) => {
      timedout.catch((err) => rej(new Error('append to followers timeout')))
      const cid = crypto.randomUUID()
      const msg = { type: APPEND, cid, data, seq, term: this.term }
      const acks = this.followers.filter((id) => this.nodeId !== id).map((id) => {
        const ack = this._awaitAck(id, cid)
        this.send(id, msg)
        return ack
      })
      awaitResolve(acks, this.minFollowers).then(() => res({ data, seq })).catch(rej)
    })
    work.catch(noop).finally(() => clearTimeout(timer))
    return work
  }

  async _appendToSelfAndFollowers(data) {
    const need = this.minFollowers
    const have = this.followers.length - 1
    if (have < need) { throw new Error(`append to self needs ${need} followers have ${have}`) }
    const work = Array.isArray(data) ? this.log.appendBatch(data) : this.log.append(data)
    return work.then((ok) => this._appendToFollowers(ok.data, ok.seq))
  }

  async append(data) {
    if (!isObj(data)) { throw new Error('data must be object') }
    if (this._stopped) { throw new Error('raft node is stopped') }
    return this.leader !== this.nodeId ? this._fwdToLeader(data) : this._appendToSelfAndFollowers(data)
  }

  async appendBatch(data) {
    if (!Array.isArray(data)) { throw new Error('data must be array') }
    const ok = data.every(isObj)
    if (!ok) { throw new Error('data must be array of objects') }
    if (this._stopped) { throw new Error('raft node is stopped') }
    return this.leader !== this.nodeId ? this._fwdToLeader(data) : this._appendToSelfAndFollowers(data)
  }
}

const sha256 = (obj) => crypto.createHash('sha256').update(JSON.stringify(obj)).digest('hex')

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
    const seq = log.seq + i
    enforceChain({ head, seq }, arr[i])
  }
}

// todo: sqlite with timeouts
class TinyRaftLog {
  constructor(opts={}) {
    opts = { ...defaults, ...opts }
    this.logTimeout = opts.logTimeout
    this._open = false
    this.seq = null
    this.log = null
    this.head = null
  }

  open() {
    return this._open
  }

  async start() {
    if (this._open) { return }
    this.seq = '-1'
    this.log = []
    this.head = null
    this._open = true
  }

  async stop() {
    if (!this._open) { return }
    this.seq = this.log = this.head = null
    this._open = false
  }

  async append(data, seq=null) {
    const next = new Decimal(this.seq).add(1).toString()
    seq = seq !== null ? seq : next
    if (!isObj(data)) { throw new Error('data must be object') }
    if (typeof seq !== 'string') { throw new Error('seq must be string') }
    if (isNaN(parseInt(seq))) { throw new Error('seq must be string number') }
    if (next !== seq) { throw new Error(`log append next ${next} !== seq ${seq}`) }
    if (!this._open) { throw new Error('log is not open') }
    enforceChain(this, data)
    this.log.push(data)
    this.head = this.log[next]
    this.seq = seq
    return { data, seq }
  }

  async appendBatch(data, seq=null) {
    const next = new Decimal(this.seq).add(1).toString()
    seq = seq !== null ? seq : next
    if (!Array.isArray(data)) { throw new Error('data must be array') }
    if (data.length <= 0) { throw new Error('data must be array with length >= 1') }
    if (typeof seq !== 'string') { throw new Error('seq must be string') }
    if (isNaN(parseInt(seq))) { throw new Error('seq must be string number') }
    if (next !== seq) { throw new Error(`log append next ${next} !== seq ${seq}`) }
    if (!this._open) { throw new Error('log is not open') }
    enforceChainArr(this, data)
    this.log = this.log.concat(data)
    this.seq = new Decimal(this.seq).add(data.length).toString()
    this.head = this.log[this.seq]
    return { data, seq }
  }

  async remove(seq) {
    if (typeof seq !== 'string') { throw new Error('seq must be string') }
    if (isNaN(parseInt(seq))) { throw new Error('seq must be string number') }
    if (!this._open) { throw new Error('log is not open') }
    seq = new Decimal(seq)
    if (seq.lessThan(0)) { throw new Error('seq must be >= 0') }
    this.seq = new Decimal(this.seq)
    if (seq.greaterThan(this.seq)) { return '0' }
    this.log = this.log.slice(0, seq.toNumber())
    const removed = this.seq.sub(seq).add(1)
    this.seq = seq.sub(1).toString()
    this.head = this.log[parseInt(this.seq)]
    return removed.toString()
  }
}

module.exports = {
  TinyRaftPlus,
  TinyRaftLog
}
