const test = require('tape')
const { FsLog } = require('../lib/fslog.js')

const toBuf = (obj) => {
  if (obj === null) { return null }
  obj = JSON.stringify(obj)
  return Buffer.from(obj, 'utf8')
}

const toObj = (buf) => {
  if (buf === null) { return null }
  return JSON.parse(buf.toString('utf8'))
}

test('test append three then iter', async (t) => {
  t.plan(2)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  await log.append(toBuf({ a: 1 }))
  await log.append(toBuf({ b: 2 }))
  await log.append(toBuf({ c: 3 }))

  let seq = 0
  for await (let next of log.iter(seq.toString())) {
    next = toObj(next)
    console.log(seq, next)
    seq++
  }

  t.pass('no errors')
  t.equal(seq, 3, 'read three bufs')
  t.teardown(() => log.stop())
})

test('test append one then iter', async (t) => {
  t.plan(2)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  await log.append(toBuf({ c: 3 }))

  let seq = 0
  for await (let next of log.iter(seq.toString())) {
    next = toObj(next)
    console.log(seq, next)
    seq++
  }

  t.pass('no errors')
  t.equal(seq, 1, 'read 1 buf')
  t.teardown(() => log.stop())
})
