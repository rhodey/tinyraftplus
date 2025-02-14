const noop = () => {}

class ConcurrentLog {
  constructor(log) {
    this.log = log
    this._prev = Promise.resolve(1)
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
    return this._prev = this._prev
      .catch(noop)
      .then(() => this.log.start())
      .then(() => this._update())
  }

  stop() {
    return this._prev = this._prev
      .catch(noop)
      .then(() => this.log.stop())
      .then(() => this._update())
  }

  append(data, seq) {
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

const restart = async (log) => {
  await log.stop()
  await log.start()
}

class AutoRestartLog {
  constructor(log) {
    this.log = log
    this._restart = false
    this._update()
  }

  open() {
    return this.log.open()
  }

  _update() {
    this.seq = this.log.seq
    this.head = this.log.head
  }

  async start() {
    await this.log.start()
    this._update()
  }

  async stop() {
    await this.log.stop()
    this._update()
  }

  async _restartIfNeeded() {
    if (!this._restart) { return }
    await restart(this.log).then(() => {
      this._update()
      this._restart = false
    }).catch((err) => {
      err.message = `${this.log.path} restart failed ${err.message}`
      throw err
    })
  }

  async append(data, seq) {
    await this._restartIfNeeded()
    return this.log.append(data, seq).then((res) => {
      this._update()
      return res
    }).catch((err) => {
      this._restart = true
      throw err
    })
  }

  async appendBatch(data, seq) {
    await this._restartIfNeeded()
    return this.log.appendBatch(data, seq).then((res) => {
      this._update()
      return res
    }).catch((err) => {
      this._restart = true
      throw err
    })
  }

  async truncate(seq=-1n) {
    await this._restartIfNeeded()
    return this.log.truncate(seq).then(() => {
      this._update()
    }).catch((err) => {
      this._restart = true
      throw err
    })
  }

  iter(seq=0n, opts={}) {
    return this.log.iter(seq, opts)
  }

  del() {
    return this.log.del()
  }
}

module.exports = { ConcurrentLog, AutoRestartLog }
