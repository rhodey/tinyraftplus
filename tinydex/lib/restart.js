const noop = () => {}

const restart = async (log) => {
  await log.stop()
  await log.start()
}

class AutoRestartLog {
  constructor(log, errorCb=noop) {
    this.log = log
    this.errorCb = errorCb
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
    await restart(this.log).catch((err) => {
      this.errorCb(err)
      throw err
    })
    this._update()
    this._restart = false
  }

  async append(data, seq=null) {
    await this._restartIfNeeded()
    const result = this.log.append(data, seq).catch((err) => {
      this._restart = true
      throw err
    })
    this._update()
    return result
  }

  async appendBatch(data, seq=null) {
    await this._restartIfNeeded()
    const result = this.log.appendBatch(data, seq).catch((err) => {
      this._restart = true
      throw err
    })
    this._update()
    return result
  }

  async truncate(seq=-1n) {
    await this._restartIfNeeded()
    const result = this.log.truncate(seq).catch((err) => {
      this._restart = true
      throw err
    })
    this._update()
    return result
  }

  iter(seq=0n, opts={}) {
    return this.log.iter(seq, opts)
  }

  del() {
    return this.log.del()
  }
}

module.exports = AutoRestartLog
