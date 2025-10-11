const crypto = require('crypto')
const EventEmitter = require('events')
const combinations = require('combinations')
// https://raft.github.io/raft.pdf

// states
const FOLLOWER = 'follower'
const CANDIDATE = 'candidate'
const LEADER = 'leader'

// rpc types
const VOTE_REQUEST = 'vote_request'
const VOTE = 'vote'
const APPEND = 'append'
const READ = 'read'
const ACK = 'ack'
const ERR = 'err'

const noop = () => {}
const ready = Promise.resolve()
const max = (a, b) => a > b ? a : b
const min = (a, b) => a < b ? a : b
const rand = (min, max) => Math.floor(Math.random() * (max - min)) + min

const terr = new Error('timeout')

// use less OS timers by group 100ms
const timeout = (ms) => {
  let timer = null
  const timedout = new Promise((res, rej) => {
    const now = Date.now()
    let next = now + ms
    next = next - (next % 100)
    next = (100 + next) - now
    timer = setTimeout(rej, next, terr)
  })
  return [timer, timedout]
}

const awaitResolve = (promises, quorum, quorumErr) => {
  return new Promise((res, rej) => {
    const [ok, err] = [[], []]
    promises.forEach((promise) => {
      promise.then((id) => {
        ok.push(id)
        if (!quorum(ok)) { return }
        res()
      }).catch((id) => {
        err.push(id)
        if (!quorumErr(err)) { return }
        rej()
      })
    })
  })
}

const awaitChange = (node, fn) => {
  return new Promise((res, rej) => {
    const cb = (state) => {
      if (!fn(state)) { return }
      node.removeListener('change', cb)
      res()
    }
    node.on('change', cb)
  })
}

const defaults = {
  // how long before nodes try to elect themselves
  electionTimeout: 1_500,
  // how long before leader expires a follower
  pingTimeout: 1_500,
  // how long before throw error for a append
  appendTimeout: 1_500,
  // how long before throw error for a read
  readTimeout: 1_500,
  // send <= rpcMax bufs per rpc
  rpcMax: 1024,
  // state machine = optional
  apply: undefined,
  // apply <= applyMax bufs per call
  applyMax: 1024,
  // state machine = optional
  read: noop,
  // node group = optional
  group: undefined,
  // called with [{id, group, state}, ...]
  // return true if nodes are quorum
  groupFn: undefined,
  // alternative to customize quorum
  quorum: undefined,
}

class RaftNode extends EventEmitter {
  constructor(id, nodes, send, log, opts={}) {
    opts = typeof opts === 'function' ? opts() : opts
    opts = { ...defaults, ...opts }
    super()
    this.id = id
    this.nodes = [...new Set(nodes)].sort()
    this.send = (to, msg) => {
      if (this._closing) { return }
      try {
        msg.from = this.id
        const ok = send(to, msg)
        if (!(ok instanceof Promise)) { return }
        ok.catch((err) => this.emit('warn', err))
      } catch (err) {
        this.emit('warn', err)
      }
    }
    this.log = log
    this.state = null
    this.leader = null
    this.followers = []
    this.term = null
    this.seq = null
    this.head = null
    this._pingms = 0
    this._pongs = new Map()
    this._votes = []
    this._acks = new Map()
    this._open = false
    this._closing = true
    const min = Math.ceil(nodes.length / 2)
    this.quorum = opts.quorum ?? min
    this._groups = new Map()
    this.group = opts.group ?? 'default'
    this.opts = opts
    this.opts.rpcMax = BigInt(opts.rpcMax)
    this._commitSeq = -1n
    this._applySeq = -1n
    this._applyPrev = ready
    this._next = new Map()
    this._match = new Map()
    this._inflight = new Set()
  }

  get isOpen() {
    return this._open
  }

  _readHead(keepTerm=false) {
    this.seq = this.log.seq
    if (!this.log.head) {
      this.term = keepTerm ? this.term : 0n
      this.log.term = -1n
      this.head = null
      return
    }
    this.log.term = this.log.head.readBigUInt64LE()
    this.term = keepTerm ? this.term : this.log.term
    this.head = this.log.head.subarray(8)
  }

  _isQuorum(ids=null) {
    ids = ids ?? this.followers
    const arr = ids.filter((id) => id !== this.id).map((id) => {
      const group = this._groups.get(id)
      return { id, group, state: FOLLOWER }
    })
    this.state === CANDIDATE && arr.push({ id: this.id, group: this.group, state: CANDIDATE })
    this.state === LEADER && arr.push({ id: this.id, group: this.group, state: LEADER })
    if (this.opts.groupFn) { return this.opts.groupFn(arr) }
    return arr.length >= this.quorum
  }

  _isQuorumErr(ids=null) {
    ids = ids ?? this.followers
    const arr = ids.filter((id) => id !== this.id).map((id) => {
      const group = this._groups.get(id)
      return { id, group, state: FOLLOWER }
    })
    if (this.opts.groupFn) { return this.opts.groupFn(arr) }
    return arr.length >= this.quorum
  }

  _startElectionTimer() {
    clearTimeout(this._electionTimer)
    const delay = () => {
      const r = Math.floor(this.opts.electionTimeout * 0.20)
      return this.opts.electionTimeout + rand(0, r)
    }
    let timeout = delay()
    const cb = () => {
      if (this.state === LEADER) { return }
      if ((Date.now() - this._pingms) >= timeout) { this._voteForSelf() }
      timeout = delay()
      this._electionTimer = setTimeout(cb, timeout)
    }
    this._electionTimer = setTimeout(cb, timeout)
  }

  _reset() {
    this.state = null
    this.leader = null
    this.followers = []
    this.term = null
    this.seq = null
    this.head = null
    this._pingms = 0
    this._votes = []
    this._pongs.clear()
    this._acks.clear()
    this._groups.clear()
    this._commitSeq = -1n
    this._applySeq = -1n
    this._next.clear()
    this._match.clear()
    clearTimeout(this._electionTimer)
    clearInterval(this._pingTimer)
  }

  async open() {
    if (this._open) { return }
    await this.log.open()
    this._reset()
    this.state = FOLLOWER
    this._readHead()
    this._closing = false
    this._open = true
    this._startElectionTimer()
    this._change()
  }

  _pruneFollowers() {
    const rm = this.nodes.filter((id) => {
      let delay = this._pongs.get(id) ?? 0
      delay = Date.now() - delay
      return delay >= this.opts.pingTimeout
    })
    this.followers = this.followers.filter((id) => !rm.includes(id))
    this.followers = this.followers.filter((id) => this.nodes.includes(id))
    return this._isQuorum()
  }

  _toFollower(leader=null) {
    this.state = FOLLOWER
    this.leader = leader
    this.followers = []
    this._votes = []
    this._pingms = 0
    this._pongs.clear()
    clearInterval(this._pingTimer)
    this._startElectionTimer()
    this._change()
  }

  async close() {
    if (!this._open) { return }
    this._closing = true
    await this._applyPrev
    await this.log.close()
    this._reset()
    this._open = false
    this._change()
  }

  async awaitLeader(commit=1) {
    const { id } = this
    if (this._closing) { throw new Error(`node ${id} is not open`) }
    return new Promise((res, rej) => {
      const isFollower = (state) => state.state === FOLLOWER && state.leader !== null
      const fn = (state) => state.state === LEADER || isFollower(state)
      if (fn(this) && !commit) { return res() }
      const cb = () => {
        this.removeListener('commit', cb)
        res()
      }
      commit && this.on('commit', cb)
      return awaitChange(this, fn).then(() => {
        if (commit) { return }
        res()
      })
    })
  }

  async awaitEvent(event, fn) {
    const { id } = this
    if (this._closing) { throw new Error(`node ${id} is not open`) }
    return new Promise((res, rej) => {
      const cb = (val) => {
        if (!fn(val)) { return }
        this.removeListener(event, cb)
        res(val)
      }
      this.on(event, cb)
    })
  }

  _change() {
    const { id, nodes, state, leader, followers, term } = this
    const change = { id, nodes, state, leader, followers, term }
    this.emit('change', change)
  }

  _voteForSelf() {
    this.state = CANDIDATE
    this.leader = this.id
    this.followers = []
    const term = ++this.term
    this._votes = []
    this._pingms = 0
    this._pongs.clear()
    this._change()
    const termP = this.log.term
    const seqP = this.log.seq
    const msg = { type: VOTE_REQUEST, term, termP, seqP }
    this.nodes.filter((id) => id !== this.id).forEach((to) => this.send(to, msg))
  }

  _rxVoteRequest(msg, from) {
    const { term, termP, seqP } = msg

    if (term === this.term && this.leader === from) {
      msg = { type: VOTE, term: this.term, voteGranted: true, group: this.group }
      this.send(from, msg)
      this._pingms = Date.now()
      return
    } else if (term < termP || term < this.term || (term === this.term && this.leader !== null)) {
      msg = { type: VOTE, term: this.term, voteGranted: false }
      this.send(from, msg)
      return
    }

    // term > this.term || (term === this.term && this.leader === null)
    const termPF = this.log.term
    const seqPF = this.log.seq

    if (term > this.term) {
      msg = { type: VOTE, term, voteGranted: true, group: this.group }
    } else if (seqP === -1n && seqPF === -1n) {
      msg = { type: VOTE, term, voteGranted: true, group: this.group }
    } else if (termP > termPF || (termP === termPF && seqP >= seqPF)) {
      msg = { type: VOTE, term, voteGranted: true, group: this.group }
    } else {
      msg = { type: VOTE, term, voteGranted: false }
      this.send(from, msg)
      return
    }

    this.term = term
    this.leader = from
    this._pingms = Date.now()
    this._toFollower(this.leader)
    this.send(from, msg)
  }

  _startPingTimer() {
    clearInterval(this._pingTimer)
    const cb = () => {
      if (this.state !== LEADER) { return }
      const termP = this.log.term
      const seqP = this.log.seq
      const commitSeq = this._commitSeq
      const cid = crypto.randomUUID()
      const msg = { type: APPEND, term: this.term, termP, seqP, commitSeq }
      this.nodes.filter((id) => id !== this.id).forEach((to, idx) => {
        let [timer, timedout] = timeout(this.opts.pingTimeout)
        timedout = timedout.catch(() => Promise.reject(new Error(`node ${this.id} node ${to} ping timeout`)))
        const msgg = { ...msg, cid: cid + idx }
        this._awaitAck(to, msgg.cid, timedout)
          .catch((err) => this.emit('warn', err))
          .finally(() => clearTimeout(timer))
        this.send(to, msgg)
      })
      if (this._pruneFollowers()) { return }
      this._toFollower()
    }
    const interval = this.opts.pingTimeout * 0.3
    this._pingTimer = setInterval(cb, interval)
    cb()
  }

  _rxVote(msg, from) {
    const { term, voteGranted, group } = msg
    if (term > this.term) {
      this.term = term
      this._toFollower()
      return
    }
    if (this.state !== CANDIDATE && this.state !== LEADER) { return }
    if (this.term !== term) { return }
    if (!voteGranted) { return }
    this._votes.push(from)
    this._votes = [...new Set(this._votes)].sort()
    this._groups.set(from, group)
    if (!this._isQuorum(this._votes)) { return }
    const change = this.state !== LEADER
    const update = change || this.followers.join(',') !== this._votes.join(',')
    this.state = LEADER
    this.leader = this.id
    this.followers = this._votes
    this._pongs.set(from, Date.now())
    clearTimeout(this._electionTimer)
    if (change) {
      this._next.clear()
      this._match.clear()
      this.nodes.forEach((id) => {
        this._pongs.set(id, Date.now())
        this._next.set(id, this.seq + 1n)
        this._match.set(id, -1n)
      })
      this._startPingTimer()
      this._leaderAppendNoOp().catch(noop)
    }
    update && this._change()
  }

  // allows discover commitSeq
  // raft paper explains this
  async _leaderAppendNoOp() {
    const { id } = this
    if (this._closing) { throw new Error(`node ${id} is not open`) }
    if (this.state !== LEADER) { throw new Error(`node ${id} is not leader`) }
    const buf = Buffer.alloc(0)
    this._appendToSelfAndFollowers(buf).catch((err) => {
      this._leaderAppendNoOp().catch(noop)
    })
  }

  _rxAppendToLeader(msg, from) {
    const { term, cid, data } = msg
    const error = (msg) => {
      msg = { type: ERR, term: this.term, cid, msg }
      this.send(from, msg)
    }

    if (term > this.term) {
      this.term = term
      this._toFollower()
      return error('term mismatch')
    } else if (this.term !== term) {
      return error('term mismatch')
    } else if (!this.followers.includes(from)) {
      return error('you are not a follower')
    }

    this._appendToSelfAndFollowers(data).then((ok) => {
      let seq = undefined
      let results = undefined
      if (Array.isArray(ok)) {
        seq = ok[0]
        results = ok[1]
      } else {
        seq = ok
      }
      msg = { type: ACK, term: this.term, cid, seq, results }
      this.send(from, msg)
    }).catch((err) => error(err.message))
  }

  // begin and end for results
  _apply(cb, rbegin=null, end=null) {
    end = end === null ? this._commitSeq : end
    end = min(end, this._commitSeq)
    if (!this.opts.apply) {
      this._applySeq = end
      return cb()
    }
    const apply = async () => {
      if (this._closing) { return }
      if (this._applySeq >= end) { return cb() }
      let results = []
      let next = this._applySeq + 1n
      rbegin = rbegin === null ? next : rbegin
      const ridx = Number(rbegin - next)
      try {

        if (this.seq === next) {
          try {
            let ok = this.head.length ? this.head : null
            ok = this.opts.apply([ok])
            if (ok instanceof Promise) { ok = await ok }
            results.push(ok[0])
          } catch (err) {
            err.message += '_apply_'
            throw err
          }
          this._applySeq = next
          this.emit('apply', next)
          return cb(results.slice(ridx))
        }

        let arr = []
        // prevent read too much into mem
        const max = this.opts.applyMax

        const apply = async (next) => {
          if (arr.length <= 0) { return }
          try {
            arr = arr.map((buf) => buf.length ? buf : null)
            let ok = this.opts.apply(arr)
            if (ok instanceof Promise) { ok = await ok }
            results = results.concat(ok)
          } catch (err) {
            err.message += '_apply_'
            throw err
          }
          this._applySeq = next
          this.emit('apply', next)
          arr = []
        }

        for await (let buf of this.log.iter(next)) {
          if (this._closing) { break }
          if (next > end) { break }
          buf = buf.subarray(8)
          arr.push(buf)
          if (arr.length >= max) { await apply(next) }
          if (this._closing) { break }
          next++
        }
        await apply(--next)
        cb(results.slice(ridx))

      } catch (err) {
        this._readHead(true)
        if (err.message.includes('_apply_')) {
          err.message = err.message.replace('_apply_', '')
          // apply errors get warn
          this.emit('warn', err)
        } else {
          // others error
          this.emit('error', err)
        }
        cb()
      }
    }
    this._applyPrev = this._applyPrev.catch(noop).then(apply)
  }

  async _rxAppendToFollower(msg, from) {
    const { cid, term, termP, seqP, commitSeq, data } = msg
    const seq = seqP + 1n
    const ack = (res=[]) => {
      msg = { type: ACK, term: this.term, cid, seq, res }
      this.send(from, msg)
    }
    const error = (msg) => {
      msg = { type: ERR, term: this.term, cid, msg }
      this.send(from, msg)
    }

    if (term > this.term) {
      this.term = term
      this._toFollower()
      return error('term mismatch')
    } else if (this.leader !== from) {
      this._rxVoteRequest(msg, from)
      return error('leader mismatch')
    } else if (term !== this.term) {
      return error('term mismatch')
    } else if (this.state !== FOLLOWER) {
      return error('not a follower')
    }

    this._pingms = Date.now()

    const apply = (ok) => {
      if (this._closing) { return }
      if (!ok) { return ack() }
      if (commitSeq <= this._commitSeq) { return ack() }
      const next = min(commitSeq, this.seq)
      if (next <= this._commitSeq) { return ack() }
      this._commitSeq = next
      this.emit('commit', next)
      this._apply(ack)
    }

    const termPF = this.log.term
    const seqPF = this.log.seq
    const ok = termPF === termP && seqPF === seqP

    if (!data) { return apply(ok) }

    const append = (seq, txn) => {
      txn.appendBatch(data, seq).then(() => txn.commit()).then(() => {
        this._readHead(true)
        apply(true)
      }).catch((err) => {
        this._readHead(true)
        txn.abort().catch(noop).finally(() => {
          this._readHead(true)
          this.emit('error', err)
          error(err.message)
        })
      })
    }

    let txn = null

    try {

      txn = await this.log.txn()
      let termPF = this.log.term
      const seqPF = this.log.seq
      const ok = termPF === termP && seqPF === seqP
      if (ok) {
        append(seq, txn)
        txn = null
        return
      }

      let have = []
      const begin = max(seqP, 0n)
      for await (let next of this.log.iter(begin, { txn })) {
        have.push(next)
        if (have.length > data.length) { break }
      }

      if (seqP >= 0n && have.length <= 0) { return error(`seqP ${seqP} missing`) }
      termPF = seqP >= 0n ? have[0].readBigUInt64LE() : -1n
      if (termPF !== termP) { return error('termP mismatch') }
      seqP >= 0n && (have = have.slice(1))
      let trim = seqP
      for (let i = 0; i < data.length; i++) {
        if (!have[i] || !have[i].equals(data[i])) { break }
        data.shift()
        trim++
      }

      await txn.lock(trim)
      this.log.trim(trim, txn)
        .then((txn) => append(++trim, txn))
      txn = null

    } catch (err) {
      this._readHead(true)
      this.emit('error', err)
      error(err.message)
    } finally {
      txn && txn.abort().catch(noop)
    }
  }

  _rxReadToLeader(msg, from) {
    const { term, cid, cmd } = msg
    const error = (msg) => {
      msg = { type: ERR, term: this.term, cid, msg }
      this.send(from, msg)
    }

    if (term > this.term) {
      this.term = term
      this._toFollower()
      return error('term mismatch')
    } else if (this.term !== term) {
      return error('term mismatch')
    } else if (!this.followers.includes(from)) {
      return error('you are not a follower')
    }

    this._leaderRead(cmd).then((arr) => {
      const [seq, result] = arr
      msg = { type: ACK, term: this.term, cid, seq, result }
      this.send(from, msg)
    }).catch((err) => error(err.message))
  }

  onReceive(from, msg) {
    if (!this._open) { return }
    if (this._closing) { return }
    const { term } = msg
    switch (msg.type) {
      case ACK:
      case ERR:
        if (term > this.term) {
          this.term = term
          this._toFollower()
        }
        break

      case VOTE_REQUEST:
        this._rxVoteRequest(msg, from)
        break

      case VOTE:
        this._rxVote(msg, from)
        break

      case APPEND:
        if (this.state === LEADER) {
          this._rxAppendToLeader(msg, from)
        } else {
          this._rxAppendToFollower(msg, from)
        }
        break

      case READ:
        if (this.state === LEADER) {
          this._rxReadToLeader(msg, from)
        }
        break

    }
    const cb = this._acks.get(msg.cid)
    cb && cb(from, msg)
  }

  _awaitAck(from, cid, timedout) {
    return new Promise((res, rej) => {
      const cb = (fromm, msg) => {
        if (ACK !== msg.type && ERR !== msg.type) { return }
        if (from !== fromm) { return }
        this._acks.delete(cid)
        if (ACK === msg.type) {
          this._pongs.set(from, Date.now())
          return res(msg)
        }
        rej(new Error(`node ${from} ${msg.msg}`))
      }
      this._acks.set(cid, cb)
      timedout.catch((err) => {
        this._acks.delete(cid)
        if (this._closing) { return }
        if (err.message === 'timeout') {
          rej(new Error(`node ${this.id} node ${from} ack timeout`))
        } else {
          rej(err)
        }
      })
    })
  }

  _fwdToLeader(data=null, cmd=null) {
    const { id } = this
    let type = APPEND
    let which = 'append'
    let timeoutms = this.opts.appendTimeout
    if (data === null) {
      type = READ
      which = 'read'
      timeoutms = this.opts.readTimeout
    }
    let [timer, timedout] = timeout(timeoutms)
    const work = new Promise((res, rej) => {
      timedout = timedout.catch(() => Promise.reject(new Error(`node ${id} fwd ${which} to leader timeout`)))
      if (this.leader === null) { return rej(new Error(`node ${id} fwd ${which} no leader`)) }
      const cid = crypto.randomUUID()
      const msg = { type, term: this.term, cid, data, cmd }
      const ack = this._awaitAck(this.leader, cid, timedout)
      const cb = (msg) => {
        if (type === READ) {
          res([msg.seq, msg.result])
        } else if (msg.results) {
          res([msg.seq, msg.results])
        } else {
          res(msg.seq)
        }
      }
      ack.then(cb).catch(rej)
      this.send(this.leader, msg)
    })
    work.catch(noop).finally(() => clearTimeout(timer))
    return work
  }

  _appendToFollower(to, seq, seqH) {
    const { id } = this
    const [timer, timedout] = timeout(this.opts.appendTimeout)
    const work = new Promise(async (res, rej) => {
      timedout.catch(noop)
      if (this._closing) {
        process.nextTick(() => rej(new Error(`node ${id} is not open`)))
        return
      } else if (!this.followers.includes(to)) {
        process.nextTick(() => rej(new Error(`node ${id} node ${to} is not my follower`)))
        return
      } else if (this._inflight.has(to)) {
        process.nextTick(() => rej(new Error(`node ${id} node ${to} has inflight`)))
        return
      }

      let termP = null
      const seqP = seq - 1n
      const data = []

      termP = seqP < 0n ? -1n : null
      const begin = seqP < 0n ? seq : seqP
      this._inflight.add(to)

      // prevent read too much into mem
      let count = 1n + seqH - seq
      count = min(this.opts.rpcMax, count)

      try {

        for await (let buf of this.log.iter(begin)) {
          termP !== null && data.push(buf)
          termP = termP ?? buf.readBigUInt64LE()
          if (BigInt(data.length) >= count) { break }
        }

      } catch (err) {
        this._inflight.delete(to)
        this.emit('error', err)
        process.nextTick(() => rej(err))
        return
      }

      if (BigInt(data.length) < count) {
        this._inflight.delete(to)
        process.nextTick(() => rej(new Error(`node ${id} read ${data.length} wanted ${count}`)))
        return
      }

      const cid = crypto.randomUUID()
      const commitSeq = this._commitSeq
      const msg = { cid, type: APPEND, term: this.term, termP, seqP, commitSeq, data }

      // no success
      const retry = (err) => {
        seq = max(0n, --seq)
        this._next.set(to, seq)
        rej([null, err])
      }

      this._awaitAck(to, msg.cid, timedout).then((msg) => {
        this._inflight.delete(to)
        const seqHH = seqP + count
        this._match.set(to, seqHH)
        const next = 1n + seqHH
        this._next.set(to, next)
        this._checkCommit()
        if (seqHH === seqH) { return res(msg) }
        // partial success due to count
        rej([seqHH])
      }).catch(retry)
      this.send(to, msg)
    })
    work.catch(noop).finally(() => clearTimeout(timer))
    return work.catch((err) => {
      if (!Array.isArray(err)) {
        this.emit('warn', err)
        return Promise.reject(err)
      }
      this._inflight.delete(to)
      const seqHH = err[0]
      err = err[1]
      err = err ?? new Error(`node ${id} follower ${to} appended ${seqHH} wanted ${seqH}`)
      this.emit('warn', err)
      const next = this._next.get(to)
      this._appendToFollower(to, next, seqH).catch(noop)
      return Promise.reject(err)
    })
  }

  _checkCommit() {
    if (this.state !== LEADER) { return }
    const nodes = this.followers.map((id) => {
      const group = this._groups.get(id)
      const match = this._match.get(id) ?? -1n
      return { id, group, state: FOLLOWER, match }
    })

    const ids = nodes.map((node) => node.id)
    const combo = combinations(ids)

    const matches = combo.map((arr) => {
      arr = arr.map((id) => nodes.find((node) => node.id === id))
      if (!this._isQuorum(arr)) { return -1n }
      arr.push({ id: this.id, match: this.seq })
      const maxx = arr.reduce((maxx, node) => max(maxx, node.match), -1n)
      return arr.reduce((minn, node) => min(minn, node.match), maxx)
    })

    const match = matches.reduce((maxx, m) => max(maxx, m), -1n)
    const change = match > this._commitSeq
    if (!change) { return }

    this._commitSeq = match
    this.emit('commit', match)
  }

  _appendToFollowers(seq, seqH) {
    const { id } = this
    const [timer, timedout] = timeout(this.opts.appendTimeout)
    const work = new Promise((res, rej) => {
      if (this._closing) { return rej(new Error(`node ${id} is not open`)) }
      if (this.state !== LEADER) { return rej(new Error(`node ${id} is not leader`)) }
      timedout.catch(() => rej(new Error(`node ${id} followers ack timeout`)))
      const acks = this.followers.map((to) => {
        let next = this._next.get(to)
        next = this._appendToFollower(to, next, seqH)
        return next.then(() => to).catch(() => Promise.reject(to))
      })
      const q = this._isQuorum.bind(this)
      const qErr = this._isQuorumErr.bind(this)
      awaitResolve(acks, q, qErr).then(() => {
        this._checkCommit()
        this._apply(res, seq, seqH)
      }).catch((err) => rej(new Error(`node ${id} followers ack false`)))
    })
    work.catch(noop).finally(() => clearTimeout(timer))
    return work
  }

  _appendToSelfAndFollowers(data) {
    const batch = Array.isArray(data)
    const term = Buffer.allocUnsafe(8)
    term.writeBigUInt64LE(this.term)
    data = batch ? data : [data]
    data = data.map((buf) => Buffer.concat([term, buf]))
    const work = batch ? this.log.appendBatch(data) : this.log.append(data[0])
    return work.catch((err) => {
      this._readHead(true)
      this.emit('error', err)
      return Promise.reject(err)
    }).then((seq) => {
      this._readHead(true)
      const map = (results) => batch ? results : results[0]
      const cb = (results) => this.opts.apply ? [seq, map(results)] : seq
      return this._appendToFollowers(seq, this.seq).then(cb)
    })
  }

  _leaderRead(cmd) {
    const { id } = this
    const [timer, timedout] = timeout(this.opts.readTimeout)
    const work = new Promise((res, rej) => {
      timedout.catch(() => rej(new Error(`node ${id} read timeout`)))
      const termP = this.log.term
      const seqP = this.log.seq
      const commitSeq = this._commitSeq
      const cid = crypto.randomUUID()
      // raft paper says ping followers
      const msg = { type: APPEND, term: this.term, termP, seqP, commitSeq, cid }
      const acks = this.followers.map((to, idx) => {
        let [timer, timedout] = timeout(this.opts.pingTimeout)
        timedout = timedout.catch(() => Promise.reject(new Error(`node ${id} follower ${to} ping timeout`)))
        const msgg = { ...msg, cid: cid + idx }
        const ack = this._awaitAck(to, msgg.cid, timedout)
        ack.catch((err) => this.emit('warn', err))
          .finally(() => clearTimeout(timer))
        this.send(to, msgg)
        return ack.then(() => to)
          .catch(() => Promise.reject(to))
      })
      const q = this._isQuorum.bind(this)
      const qErr = this._isQuorumErr.bind(this)
      awaitResolve(acks, q, qErr).then(() => {
        const read = async () => {
          try {
            const seq = this._applySeq
            let ok = this.opts.read(cmd)
            if (!(ok instanceof Promise)) { return res([seq, ok]) }
            ok = await ok
            res([seq, ok])
          } catch (err) {
            this.emit('warn', err)
            rej(err)
          }
        }
        this._applyPrev = this._applyPrev.catch(noop).then(read)
      }).catch((err) => rej(new Error(`node ${id} read false`)))
    })
    work.catch(noop).finally(() => clearTimeout(timer))
    return work
  }

  async append(data) {
    if (this._closing) { throw new Error(`node ${this.id} is not open`) }
    const isLeader = this.state === LEADER
    const work = isLeader ? this._appendToSelfAndFollowers(data) : this._fwdToLeader(data)
    return work
  }

  async appendBatch(data) {
    if (this._closing) { throw new Error(`node ${this.id} is not open`) }
    const isLeader = this.state === LEADER
    const work = isLeader ? this._appendToSelfAndFollowers(data) : this._fwdToLeader(data)
    return work
  }

  async read(cmd=null) {
    if (this._closing) { throw new Error(`node ${this.id} is not open`) }
    const isLeader = this.state === LEADER
    const work = isLeader ? this._leaderRead(cmd) : this._fwdToLeader(null, cmd)
    return work
  }
}

module.exports = { RaftNode }
