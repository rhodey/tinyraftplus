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
const isBigInt = (num) => typeof num === 'bigint' && num >= -1n

const terr = new Error('timeout')

// use less timers by group 100ms
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
  // allow state resume
  applySeq: -1n,
  // state machine = optional
  read: noop,
  // node group = optional
  group: undefined,
  // called with [{id, group, state}, ...]
  // return true if nodes are quorum
  groupFn: undefined,
  // alternative (quorum as number)
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
        ok.catch((err) => {
          err.message = `(send) ${err.message}`
          this.emit('warn', err)
        })
      } catch (err) {
        err.message = `(send) ${err.message}`
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
    this._applySeq = BigInt(opts.applySeq)
    this._applyPrev = ready
    this._next = new Map()
    this._match = new Map()
    this._inflight = new Set()
  }

  get isOpen() {
    return this._open
  }

  _readHead(keepTerm=false) {
    if (!this.log.isOpen) { return }
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
    this._pongs.clear()
    this._votes = []
    this._acks.clear()
    this._groups.clear()
    this._next.clear()
    this._match.clear()
    this._inflight.clear()
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
    return this._isQuorum()
  }

  _toFollower(leader=null) {
    this.state = FOLLOWER
    this.leader = leader
    this.followers = []
    this._pingms = 0
    this._pongs.clear()
    this._votes = []
    this._next.clear()
    this._match.clear()
    this._inflight.clear()
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

  async awaitLeader(commit=false) {
    if (this._closing) { throw new Error(`node ${this.id} is not open`) }
    return new Promise((res, rej) => {
      const isFollower = (state) => state.state === FOLLOWER && state.leader !== null
      const fn = (state) => state.state === LEADER || isFollower(state)
      const fn2 = (state) => fn(state) && state.term >= 0n && state.term === this.log.term
      if (fn(this) && !commit) { return res() }
      if (fn2(this)) { return res() }
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
    if (this._closing) { throw new Error(`node ${this.id} is not open`) }
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
    change.open = this._open
    this.emit('change', change)
  }

  _voteForSelf() {
    this.state = CANDIDATE
    this.leader = null
    this.followers = []
    const term = ++this.term
    this._pingms = 0
    this._pongs.clear()
    this._votes = []
    this._next.clear()
    this._match.clear()
    this._inflight.clear()
    this._change()
    const termP = this.log.term
    const seqP = this.log.seq
    const msg = { type: VOTE_REQUEST, term, termP, seqP }
    this.nodes.filter((id) => id !== this.id).forEach((to) => this.send(to, msg))
  }

  _rxVoteRequest(msg, from) {
    const { group } = this
    const { term, termP, seqP } = msg
    const nums = [term, termP, seqP]

    if (!nums.every(isBigInt)) {
      msg = { type: VOTE, term: this.term, voteGranted: false, group }
      this.send(from, msg)
      return
    } else if (term === this.term && this.leader === from) {
      msg = { type: VOTE, term: this.term, voteGranted: true, group }
      this.send(from, msg)
      this._pingms = Date.now()
      return
    } else if (term < termP || term < this.term || (term === this.term && this.leader !== null)) {
      msg = { type: VOTE, term: this.term, voteGranted: false, group }
      this.send(from, msg)
      return
    }

    // term > this.term || (term === this.term && this.leader === null)
    const termPF = this.log.term
    const seqPF = this.log.seq

    if (termP > termPF) {
      msg = { type: VOTE, term, voteGranted: true, group }
    } else if (termP === termPF && seqP >= seqPF) {
      msg = { type: VOTE, term, voteGranted: true, group }
    } else {
      msg = { type: VOTE, term: this.term, voteGranted: false, group }
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
        timedout = timedout.catch(() => Promise.reject(new Error(`node ${this.id} ping ${to} timeout`)))
        const msgg = { ...msg, cid: cid + idx }
        this._awaitAck(to, msgg, timedout)
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
    if (!isBigInt(term)) { return }
    if (term > this.term) {
      this.term = term
      this._toFollower()
      return
    }
    if (this.state !== CANDIDATE && this.state !== LEADER) { return }
    if (this.term !== term) { return }
    if (!voteGranted) { return }
    this._pongs.set(from, Date.now())
    this._votes.push(from)
    this._votes = [...new Set(this._votes)].sort()
    this._groups.set(from, group)
    if (!this._isQuorum(this._votes)) { return }
    const change = this.state !== LEADER
    const update = change || this.followers.join(',') !== this._votes.join(',')
    this.state = LEADER
    this.leader = this.id
    this.followers = [...this._votes]
    clearTimeout(this._electionTimer)
    if (change) {
      this._next.clear()
      this._match.clear()
      this._inflight.clear()
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
    if (this._closing) { return }
    if (this.state !== LEADER) { return }
    const buf = Buffer.alloc(0)
    this._appendToSelfAndFollowers(buf).catch((err) => {
      this.emit('warn', err)
      setTimeout(() => {
        this._leaderAppendNoOp().catch(noop)
      }, 100)
    })
  }

  _rxAppendToLeader(msg, from) {
    const { term, cid, data } = msg
    const error = (msg) => {
      msg = { type: ERR, term: this.term, cid, msg }
      this.send(from, msg)
    }

    if (!isBigInt(term)) {
      return error('term not bigint')
    } else if (term > this.term) {
      this.term = term
      this._toFollower()
      return error('term is greater')
    } else if (this.term !== term) {
      return error('term is lesser')
    } else if (!data) {
      return error('data is missing')
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
    if (!this.opts.apply) {
      this._applySeq = this._commitSeq
      return cb([])
    }
    end = end ?? this._commitSeq
    end = min(end, this._commitSeq)
    const apply = async () => {
      if (this._closing) { return }
      if (this._applySeq >= end) { return cb([]) }
      let results = []
      let next = this._applySeq + 1n
      rbegin = rbegin ?? next
      const ridx = Number(rbegin - next)

      try {

        if (this.seq === next) {
          try {
            let ok = this.head.length ? this.head : null
            ok = this.opts.apply([ok], next)
            if (ok instanceof Promise) { ok = await ok }
            results.push(ok[0])
          } catch (err) {
            err.message = `(apply) ${err.message}`
            throw err
          }
          this._applySeq = next
          this.emit('apply', next)
          ridx >= 1 && (results = results.slice(ridx))
          return cb(results)
        }

        let arr = []
        // prevent read too much into mem
        const max = this.opts.applyMax

        const apply = async (next) => {
          if (arr.length <= 0) { return }
          try {
            arr = arr.map((buf) => buf.length ? buf : null)
            const seq = (next + 1n) - BigInt(arr.length)
            let ok = this.opts.apply(arr, seq)
            if (ok instanceof Promise) { ok = await ok }
            results = results.concat(ok)
          } catch (err) {
            err.message = `(apply) ${err.message}`
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
          next++
          if (this._closing) { break }
        }
        await apply(--next)
        ridx >= 1 && (results = results.slice(ridx))
        cb(results)

      } catch (err) {
        this._readHead(true)
        this.emit('error', err)
        cb([])
      }
    }
    this._applyPrev = this._applyPrev.catch(noop).then(apply)
  }

  async _rxAppendToFollower(msg, from) {
    const { cid, term, termP, seqP, commitSeq, data } = msg
    const nums = [term, termP, seqP, commitSeq]

    const error = (msg) => {
      msg = { type: ERR, term: this.term, cid, msg }
      this.send(from, msg)
    }

    if (!nums.every(isBigInt)) {
      return error('term, termP, seqP, commitSeq not bigint')
    } else if (term > this.term) {
      this.term = term
      this._toFollower()
      return error('term is greater')
    } else if (from !== this.leader) {
      this._rxVoteRequest(msg, from)
      return error('leader mismatch')
    } else if (term !== this.term) {
      return error('term is lesser')
    } else if (this.state !== FOLLOWER) {
      this._rxVoteRequest(msg, from)
      return error('am not a follower')
    }

    this._pingms = Date.now()
    const seq = seqP + 1n

    const ack = () => {
      msg = { type: ACK, term: this.term, cid, seq }
      this.send(from, msg)
    }

    const apply = (ok) => {
      if (this._closing) { return }
      if (!ok) { return ack() }
      if (commitSeq <= this._commitSeq) { return ack() }
      const next = min(commitSeq, this.seq)
      if (next <= this._commitSeq) { return ack() }
      this._commitSeq = next
      this.emit('commit', next)
      this._apply(noop)
      ack()
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
        this.emit('error', err)
        error(err.message)
        txn.abort().catch(noop)
          .finally(() => this._readHead(true))
      })
    }

    let txn = null

    try {

      txn = await this.log.txn()
      let termPF = this.log.term
      const seqPF = this.log.seq
      const ok = termPF === termP && seqPF === seqP
      // todo: test that fast path happens
      if (ok) {
        append(seq, txn)
        txn = null
        return
      }

      let have = []
      const begin = max(seqP, 0n)
      for await (let next of this.log.iter(begin, { txn })) {
        have.push(next)
        if (have.length >= data.length) { break }
      }

      if (seqP >= 0n && have.length <= 0) { return error(`seqP ${seqP} not found (subtract)`) }
      termPF = seqP >= 0n ? have[0].readBigUInt64LE() : -1n
      if (termPF !== termP) { return error('termP mismatch (subtract)') }
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

    if (!isBigInt(term)) {
      return error('term not bigint')
    } else if (term > this.term) {
      this.term = term
      this._toFollower()
      return error('term is greater')
    } else if (this.term !== term) {
      return error('term is lesser')
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

  _awaitAck(from, msg, timedout) {
    const { type, cid } = msg
    return new Promise((res, rej) => {
      const cb = (fromm, msg) => {
        if (ACK !== msg.type && ERR !== msg.type) { return }
        if (from !== fromm) { return }
        this._acks.delete(cid)
        if (ACK === msg.type) {
          this._pongs.set(from, Date.now())
          return res(msg)
        }
        rej(new Error(`node ${this.id} ${type} ${from} ERR ${msg.msg}`))
      }
      this._acks.set(cid, cb)
      timedout.catch((err) => {
        this._acks.delete(cid)
        if (this._closing) { return }
        if (err.message === 'timeout') {
          rej(new Error(`node ${this.id} ${type} ${from} ACK timeout`))
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
      if (this.leader === null) { return rej(new Error(`node ${id} fwd ${which} no leader`)) }
      timedout = timedout.catch(() => Promise.reject(new Error(`node ${id} fwd ${which} ${this.leader} timeout`)))
      const cid = crypto.randomUUID()
      const msg = { type, term: this.term, cid, data, cmd }
      const ack = this._awaitAck(this.leader, msg, timedout)
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

  _appendToFollower(to, begin, end, term=null) {
    const { id } = this
    term = term ?? this.term
    let [timer, timedout] = timeout(this.opts.appendTimeout)
    timedout = timedout.catch(() => Promise.reject(new Error(`node ${id} (append follower) ${to} timeout`)))
    const work = new Promise(async (res, rej) => {
      if (this._closing) {
        return rej(new Error(`node ${id} (append follower) is not open`))
      } else if (this.state !== LEADER) {
        return rej(new Error(`node ${id} (append follower) is not leader`))
      } else if (this.term !== term) {
        return rej(new Error(`node ${id} (append follower) new term`))
      } else if (this._inflight.has(to)) {
        return rej(new Error(`node ${id} (append follower) ${to} inflight`))
      }

      this._inflight.add(to)
      const data = []
      const seqP = begin - 1n
      let termP = seqP < 0n ? -1n : null
      const b = seqP < 0n ? begin : seqP

      // prevent read too much into mem
      let count = 1n + end - begin
      count = min(this.opts.rpcMax, count)

      try {

        for await (let buf of this.log.iter(b)) {
          termP !== null && data.push(buf)
          termP = termP ?? buf.readBigUInt64LE()
          if (BigInt(data.length) >= count) { break }
        }

      } catch (err) {
        this._inflight.delete(to)
        this.emit('error', err)
        return rej(err)
      }

      if (BigInt(data.length) !== count) {
        this._inflight.delete(to)
        return rej(new Error(`node ${id} (append follower) read ${data.length} want ${count}`))
      }

      const cid = crypto.randomUUID()
      const commitSeq = this._commitSeq
      const msg = { cid, type: APPEND, term: this.term, termP, seqP, commitSeq, data }

      // no success
      const retry = (err) => {
        const sub = err.message.includes('(subtract)')
        const next = sub ? max(0n, begin - 1n) : begin
        this._next.set(to, next)
        rej([null, err])
      }

      this._awaitAck(to, msg, timedout).then((msg) => {
        const endd = seqP + count
        this._match.set(to, endd)
        const next = endd + 1n
        this._next.set(to, next)
        this._checkCommit()
        // full success
        if (endd === end) {
          this._inflight.delete(to)
          if (next > this.seq) { return res(msg) }
          // proactive sync
          this._appendToFollower(to, next, this.seq, term).catch(noop)
          res(msg)
        }
        // partial success
        const err = new Error(`node ${id} (append follower) ${to} ack ${endd} want ${end}`)
        rej([null, err])
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
      this.emit('warn', err[1])
      const next = this._next.get(to)
      this._appendToFollower(to, next, end, term).catch(noop)
      return Promise.reject(err[1])
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

  _appendToFollowers(begin, end) {
    const { id } = this
    const [timer, timedout] = timeout(this.opts.appendTimeout)
    const work = new Promise((res, rej) => {
      if (this._closing) { return rej(new Error(`node ${id} is not open`)) }
      if (this.state !== LEADER) { return rej(new Error(`node ${id} is not leader`)) }
      timedout.catch(() => rej(new Error(`node ${id} append to followers ack timeout`)))
      const acks = this.followers.map((to) => {
        let next = this._next.get(to)
        next = min(next, begin)
        next = this._appendToFollower(to, next, end)
        return next.then(() => to).catch(() => Promise.reject(to))
      })
      const q = this._isQuorum.bind(this)
      const qErr = this._isQuorumErr.bind(this)
      awaitResolve(acks, q, qErr).then(() => {
        this._checkCommit()
        // todo: time is counted here
        this._apply(res, begin, end)
      }).catch((err) => rej(new Error(`node ${id} append to followers ack false`)))
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
      const end = seq + BigInt(data.length - 1)
      const map = (results) => batch ? results : results[0]
      const cb = (results) => this.opts.apply ? [seq, map(results)] : seq
      return this._appendToFollowers(seq, end).then(cb)
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
        timedout = timedout.catch(() => Promise.reject(new Error(`node ${id} ping (read) ${to} timeout`)))
        const msgg = { ...msg, cid: cid + idx }
        const ack = this._awaitAck(to, msgg, timedout)
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
            // todo: time is counted here
            let ok = this.opts.read(cmd)
            if (!(ok instanceof Promise)) { return res([seq, ok]) }
            ok = await ok
            res([seq, ok])
          } catch (err) {
            err.message = `(read) ${err.message}`
            this.emit('error', err)
            rej(err)
          }
        }
        this._applyPrev = this._applyPrev.catch(noop).then(read)
      }).catch((err) => rej(new Error(`node ${id} read followers ack false`)))
    })
    work.catch(noop).finally(() => clearTimeout(timer))
    return work
  }

  async append(data) {
    const { id } = this
    if (this._closing) { throw new Error(`node ${id} is not open`) }
    if (!Buffer.isBuffer(data)) { throw new Error(`node ${id} data must be buf`) }
    const isLeader = this.state === LEADER
    const work = isLeader ? this._appendToSelfAndFollowers(data) : this._fwdToLeader(data)
    return work
  }

  async appendBatch(data) {
    if (this._closing) { throw new Error(`node ${id} is not open`) }
    if (data.length <= 0) { throw new Error(`node ${id} data must be array with len > 0`) }
    if (!data.every((buf) => Buffer.isBuffer(buf))) { throw new Error(`node ${id} data must be array of bufs`) }
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
