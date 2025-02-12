class BatchingLog {
  constructor(log, interval) {
    this.log = log
    this.interval = interval
    this.batch = []
    this._update()
  }

  open() {
    return this.log.open()
  }

  _update(res) {
    this.seq = this.log.seq
    this.head = this.log.head
    return res
  }

  start() {
    return this.log.start()
      .then(() => this._update())
  }

  stop() {
    return this.log.stop()
      .then(() => this._update())
  }

  append(data, seq) {
    if (typeof seq !== undefined) {

    }
    return this._prev = this._prev
      .catch(noop)
      .then(() => this.log.append(data, seq))
      .then((res) => this._update(res))
  }

  appendBatch(data, seq) {
    return this._prev = this._prev
      .catch(noop)
      .then(() => this.log.appendBatch(data, seq))
      .then((res) => this._update(res))
  }

  truncate(seq=-1n) {
    return this._prev = this._prev
      .catch(noop)
      .then(() => this.log.truncate(seq))
      .then(() => this._update())
  }

  iter(seq=0n, opts={}) {
    return this.log.iter(seq, opts)
  }

  del() {
    return this._prev = this._prev
      .catch(noop)
      .then(() => this.log.del())
      .then(() => this._update())
  }
}

module.exports = BatchingLog
