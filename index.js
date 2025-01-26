const crypto = require('crypto')
const TinyRaft = require('tinyraft')

const noop = () => {}

const defaults = {
  heartbeatTimeout: 500,
  electionTimeout: 2_500,
  leadershipTimeout: 5_000,
  initialTerm: 0, leaderPriority: 0,
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

class TinyRaftNode extends TinyRaft {
  constructor(nodeId, nodes, send, log, opts={}) {
    const minimumAcks = Math.ceil(nodes.length / 2)
    opts = { ...opts, nodeId, nodes, minimumAcks, send }
    opts = { ...defaults, ...opts }
    super(opts)
    this.minimumAcks = minimumAcks
    this.stopped = false
    this.log = log
  }

  start() {
    if (this.stopped) { throw new Error('stopped') }
    super.start()
  }

  stop() {
    this.stopped = true
    super.stop()
  }

  markAlive() {
    if (this.stopped) { return }
    super.markAlive()
  }

  onReceive(from, msg) {
    if (this.stopped) { return }
    super.emit('receive', [from, msg])
    super.onReceive(from, msg)
  }

  async _awaitLeader() {
    const leading = (state) => state.state === 'leader'
    const following = (state) => state.state === 'follower' && state.leader !== null
    if (leading(this) || following(this)) { return state.leader }
    const fn = (st) => leading(st) || following(st)
    return awaitChange(this, fn).then(() => state.leader)
  }

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

  async _fwdNext(data) {
    const cid = crypto.randomUUID()
    const msg = { type: 'next', cid, data }
    const acks = this.followers.map((id) => {
      const ack = this._awaitAck(id, cid)
      super.send(id, msg)
      return ack
    })

    return awaitResolve(acks, this.minimumAcks)
  }

  async append(data) {
    const leader = await _awaitLeader()
    if (this.nodeId === leader) {
      const next = this.log.seq + 1
      return this.log.append(next, data).then((now) => {
        if (now !== next) { throw new Error(`expected ${next} have ${now}`) }
        return this._fwdNext(data)
      })
    }

    // todo: fwd to leader
  }
}

class TinyRaftLog {
  constructor(config={}) {
    this.config = config
    this.seq = null
    this.log = null
  }

  async open() {
    // todo: load from fs
    this.seq = -1
    this.log = []
  }

  async head() {
    if (this.seq < 0) { return null }
    return this.log[this.seq]
  }

  async close() {
    this.seq = this.log = null
  }

  async append(seq, data) {
    if ((this.seq + 1) !== seq) { return this.seq }
    this.log.push(data)
    this.seq = seq
    return seq
  }

  async remove(seq) {
    if (seq > this.seq) { return 0 }
    this.log = this.log.slice(0, seq)
    const removed = 1 + (this.seq - seq)
    this.seq = seq - 1
    return removed
  }
}

function log(opts={}) {
  return new TinyLog(opts)
}

module.exports = {
  TinyRaftNode,
  TinyRaftLog
}
