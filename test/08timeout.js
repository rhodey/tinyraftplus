const test = require('tape')
const { TimeoutLog } = require('../src/index.js')

const delay = (ms, val) => {
  return new Promise((res, rej) => {
    if (val instanceof Error) {
      return setTimeout(() => rej(val), ms)
    }
    setTimeout(() => res(val), ms)
  })
}

test('test open', async (t) => {
  t.plan(2)

  let log = { open: () => delay(100) }
  log = new TimeoutLog(log, { open: 500 })

  await log.open()
  t.pass('open ok')

  log = { open: () => delay(500) }
  log = new TimeoutLog(log, { open: 200 })

  try {
    await log.open()
    t.fail('timeout not ok')
  } catch (err) {
    t.ok(err.message.includes('open timeout'), 'timeout ok')
  }
})

test('test close', async (t) => {
  t.plan(2)

  let log = { close: () => delay(100) }
  log = new TimeoutLog(log, { close: 500 })

  await log.close()
  t.pass('close ok')

  log = { close: () => delay(500) }
  log = new TimeoutLog(log, { close: 200 })

  try {
    await log.close()
    t.fail('timeout not ok')
  } catch (err) {
    t.ok(err.message.includes('close timeout'), 'timeout ok')
  }
})

test('test txn', async (t) => {
  t.plan(11)

  let txn = {
    append: () => delay(200, 111),
    appendBatch: () => delay(200, 222),
    commit: () => delay(100, 333),
    abort: () => delay(100, 444),
  }

  let log = { txn: () => delay(100, txn) }
  log = new TimeoutLog(log, { txn: 500, append: 600, appendBatch: 600 })

  txn = await log.txn()
  t.pass('txn ok')

  let val = await txn.append()
  t.equal(val, 111, 'txn append ok')
  val = await txn.appendBatch()
  t.equal(val, 222, 'txn appendBatch ok')
  val = await txn.commit()
  t.equal(val, 333, 'txn commit ok')
  val = await txn.abort()
  t.equal(val, 444, 'txn abort ok')

  log = { txn: () => delay(500) }
  log = new TimeoutLog(log, { txn: 200 })

  try {
    await log.txn()
    t.fail('timeout not ok')
  } catch (err) {
    t.ok(err.message.includes('txn timeout'), 'timeout ok')
  }

  txn = {
    append: () => delay(600),
    appendBatch: () => delay(600),
    commit: () => delay(500),
    abort: () => delay(500),
  }

  log = { txn: () => delay(100, txn) }
  log = new TimeoutLog(log, { txn: 500, append: 200, appendBatch: 200, commit: 200, abort: 200 })

  txn = await log.txn()
  t.pass('txn ok')

  try {
    await txn.append()
    t.fail('timeout append not ok')
  } catch (err) {
    t.ok(err.message.includes('txn append timeout'), 'timeout append ok')
  }

  try {
    await txn.appendBatch()
    t.fail('timeout appendBatch not ok')
  } catch (err) {
    t.ok(err.message.includes('txn appendBatch timeout'), 'timeout appendBatch ok')
  }

  try {
    await txn.commit()
    t.fail('timeout commit not ok')
  } catch (err) {
    t.ok(err.message.includes('txn commit timeout'), 'timeout commit ok')
  }

  try {
    await txn.abort()
    t.fail('timeout abort not ok')
  } catch (err) {
    t.ok(err.message.includes('txn abort timeout'), 'timeout abort ok')
  }
})

test('test append', async (t) => {
  t.plan(2)

  let log = { append: () => delay(100, 111) }
  log = new TimeoutLog(log, { append: 500 })

  const val = await log.append()
  t.equal(val, 111, 'append ok')

  log = { append: () => delay(500) }
  log = new TimeoutLog(log, { append: 200 })

  try {
    await log.append()
    t.fail('timeout not ok')
  } catch (err) {
    t.ok(err.message.includes('append timeout'), 'timeout ok')
  }
})

test('test appendBatch', async (t) => {
  t.plan(2)

  let log = { appendBatch: () => delay(100, 111) }
  log = new TimeoutLog(log, { appendBatch: 500 })

  const val = await log.appendBatch()
  t.equal(val, 111, 'appendBatch ok')

  log = { appendBatch: () => delay(500) }
  log = new TimeoutLog(log, { appendBatch: 200 })

  try {
    await log.appendBatch()
    t.fail('timeout not ok')
  } catch (err) {
    t.ok(err.message.includes('appendBatch timeout'), 'timeout ok')
  }
})

test('test trim', async (t) => {
  t.plan(2)

  let log = { trim: () => delay(100, 111) }
  log = new TimeoutLog(log, { trim: 500 })

  const val = await log.trim()
  t.equal(val, 111, 'trim ok')

  log = { trim: () => delay(500) }
  log = new TimeoutLog(log, { trim: 200 })

  try {
    await log.trim()
    t.fail('timeout not ok')
  } catch (err) {
    t.ok(err.message.includes('trim timeout'), 'timeout ok')
  }
})

const elems = [1, 2, 3]

const iterNoDelay = {
  [Symbol.asyncIterator]() {
    let i = 0
    return {
      async next() {
        await delay(100)
        if (i < elems.length) {
          return { value: elems[i++], done: false }
        }
        return { done: true }
      }
    }
  }
}

const iterDelay = {
  [Symbol.asyncIterator]() {
    let i = 0
    return {
      async next() {
        await delay(500)
        if (i < elems.length) {
          return { value: elems[i++], done: false }
        }
        return { done: true }
      }
    }
  }
}

test('test iter', async (t) => {
  t.plan(3)

  let iter = { lazy: () => iterNoDelay[Symbol.asyncIterator](), close: () => {} }
  let log = { iter: () => iter }
  log = new TimeoutLog(log, { iter: 500 })
  log.seq = BigInt(elems.length)

  let count = 0
  for await (let next of log.iter(0n)) { count++ }
  t.equal(count, elems.length, 'iter ok')

  iter = { lazy: () => iterDelay[Symbol.asyncIterator](), close: () => {} }
  log = { iter: () => iter }
  log = new TimeoutLog(log, { iter: 100 })

  try {
    count = 0
    for await (let next of log.iter(0n)) { count++ }
    t.fail('timeout not ok')
  } catch (err) {
    t.ok(err.message.includes('iter timeout'), 'timeout ok')
  }

  t.equal(count, 0, 'timeout ok')
})

test('test del', async (t) => {
  t.plan(2)

  let log = { del: () => delay(100, 111) }
  log = new TimeoutLog(log, { del: 500 })

  const val = await log.del()
  t.equal(val, 111, 'del ok')

  log = { del: () => delay(500) }
  log = new TimeoutLog(log, { del: 200 })

  try {
    await log.del()
    t.fail('timeout not ok')
  } catch (err) {
    t.ok(err.message.includes('del timeout'), 'timeout ok')
  }
})
