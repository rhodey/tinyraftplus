const crypto = require('crypto')
const TinyRaft = require('tinyraft')

const noop = () => {}

const defaults = {
  heartbeatTimeout: 500,
  electionTimeout: 2_500,
  leadershipTimeout: 5_000,
  initialTerm: 0, leaderPriority: 0,
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

class TinyRaftNode extends TinyRaft {
  constructor(nodeId, nodes, send, log, opts={}) {
    opts = { ...opts, nodeId, nodes, send }
    opts = { ...defaults, ...opts }
    super(opts)
    this.minFollowers = Math.ceil((nodes.length - 1) / 2)
    this._stopped = false
    this.log = log
  }

  open() {
    return this.log.open()
  }

  start() {
    if (this.open()) { return }
    if (this._stopped) { throw new Error('raft node stopped') }
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
    /*
    super.emit('receive', [from, msg])
    if (this.leader === from) { this.markAlive() }
    if (msg.type === 'fwd') {
      // todo: apply and ack
      return
    } else if (msg.type === 'next') {
      // todo: apply and ack
      return
    }
    */
    super.onReceive(from, msg)
  }

  // todo: timeout
  async _awaitLeader() {
    const leading = (state) => state.state === 'leader'
    const following = (state) => state.state === 'follower' && state.leader !== null
    if (leading(this) || following(this)) { return state.leader }
    const fn = (st) => leading(st) || following(st)
    return awaitChange(this, fn).then(() => state.leader)
  }

  // todo: timeout
  _awaitAck(from, cid) {
    return new Promise((res, rej) => {
      const listen = (arr) => {
        const [fromm, msg] = arr
        if (from !== fromm) { return }
        if (cid !== msg.cid) { return }
        this.removeListener('receive', listen)
        res(msg)
      }
      this.on('receive', listen)
    })
  }

  // todo: timeout
  _fwdToFollowers(data) {
    const cid = crypto.randomUUID()
    const msg = { type: 'next', cid, data }
    const acks = this.followers.filter((id) => this.nodeId !== id).map((id) => {
      const ack = this._awaitAck(id, cid)
      super.send(id, msg)
      return ack
    })
    return awaitResolve(acks, this.minFollowers)
  }

  async append(data) {
    if (this._stopped) { throw new Error('raft node stopped') }

    const leader = await _awaitLeader()
    if (this.nodeId === leader) {
      const need = this.minFollowers
      const have = this.followers.length - 1 // todo: confirm
      if (have < need) { throw new Error(`append need ${need} followers have ${have}`) }

      const next = this.log.seq + 1
      return this.log.append(next, data).then((now) => {
        if (next !== now) { throw new Error(`append expected seq ${next} have ${now}`) }
        return this._fwdToFollowers(data)
      })
    }

    const cid = crypto.randomUUID()
    const msg = { type: 'fwd', cid, data }
    const ack = this._awaitAck(leader, cid)
    super.send(leader, msg)
    return ack
  }
}

// todo: timeouts
class TinyRaftLog {
  constructor(config={}) {
    this._config = config
    this._open = false
    this.seq = null
    this.log = null
  }

  open() {
    return this._open
  }

  async start() {
    // todo: load from fs
    if (this._open) { return }
    this.seq = -1
    this.log = []
    this._open = true
  }

  async stop() {
    if (!this._open) { return }
    this.seq = this.log = null
    this._open = false
  }

  async head() {
    if (!this._open) { throw new Error('log not open') }
    if (this.seq < 0) { return null }
    return this.log[this.seq]
  }

  async append(seq, data) {
    if (!this._open) { throw new Error('log not open') }
    if ((this.seq + 1) !== seq) { return this.seq }
    this.log.push(data)
    this.seq = seq
    return seq
  }

  async remove(seq) {
    if (!this._open) { throw new Error('log not open') }
    if (seq > this.seq) { return 0 }
    this.log = this.log.slice(0, seq)
    const removed = 1 + (this.seq - seq)
    this.seq = seq - 1
    return removed
  }
}

module.exports = {
  TinyRaftNode,
  TinyRaftLog
}
