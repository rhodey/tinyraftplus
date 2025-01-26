const crypto = require('crypto')
const TinyRaft = require('tinyraft')

const defaults = {
  heartbeatTimeout: 500,
  electionTimeout: 2_500,
  leadershipTimeout: 5_000,
  initialTerm: 0, leaderPriority: 0,
}

function open(nodeId, nodes, send, opts={}) {
  const minAcks = Math.ceil(nodes.length / 2)
  opts = { ...opts, nodeId, nodes, minAcks, send }
  opts = { ...defaults, ...opts }
  return new TinyRaftLog(opts)
}

function awaitChange(node, fn) {
  return new Promise((res, rej) => {
    node.on('change', (st) => {
      if (fn(st)) { res() }
    })
  })
}

const noop = () => {}

function awaitResolve(arr, min) {
  let count = 0
  return new Promise((res, rej) => {
    arr.forEach((promise) => {
      promise.then(() => {
        if (++count < min) { return }
        res()
      }).catch(noop)
    })
  })
}

class TinyRaftLog extends TinyRaft {
  constructor(config) {
    super(config)
    this.stopped = false
    this.minAcks = opts.minAcks
    this.log = []
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

  async _awaitReceive(cid) {
    return new Promise((res, rej) => {
      const fn = (arr) => {
        const [from, msg] = arr
        if (cid !== msg.cid) { return }
        this.removeListener('receive', fn)
        res(msg)
      }
      this.on('receive', fn)
    })
  }

  _sendRpc(to, data) {
    const cid = crypto.randomUUID()
    const msg = { ...data, type: 'logrpc', cid }
    const acks = this._awaitReceive(cid)
    super.send(to, msg)
    await awaitResolve(acks, this.minAcks)
  }

  async append(data) {
    const leader = await _awaitLeader()
    if (this.nodeId === leader) {
      this.log.push(data)
      return this._sendRpc(data)
    }
  }
}

module.exports = {
  open,
}
