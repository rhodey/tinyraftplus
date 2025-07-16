const test = require('tape')
const { FsLog } = require('../lib/fslog.js')
const { Encoder, XxHashEncoder } = require('../lib/encoders.js')

const toBuf = (obj) => {
  if (obj === null) { return null }
  obj = JSON.stringify(obj)
  return Buffer.from(obj, 'utf8')
}

async function testAppendSmall(t, encoder) {
  t.plan(1)
  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.open()

  const data = []
  const count = 100
  for (let i = 0; i < count; i++) {
    data.push(toBuf({ i }))
  }

  const begin = Date.now()
  for (const buf of data) {
    await log.append(buf)
  }

  const ms = Date.now() - begin
  console.log(`done ${count} in ${ms}ms`)

  const seconds = ms / 1000
  let avg = (count / seconds).toFixed(1)
  console.log(`${avg} append per second`)

  avg = (avg / 1000).toFixed(2)
  console.log(`${avg} append per ms`)

  t.pass('ok')
  t.teardown(() => log.close())
  console.log(`\n`)
}

test('test append 100 small', (t) => testAppendSmall(t, new Encoder()))
test('test append 100 small - xxhash body', (t) => testAppendSmall(t, new XxHashEncoder()))
test('test append 100 small - xxhash no body', (t) => testAppendSmall(t, new XxHashEncoder(false)))

async function testAppendLarge(t, encoder) {
  t.plan(1)
  const opts = { encoder }
  const log = new FsLog('/tmp/', 'test', opts)
  await log.del()
  await log.open()

  const data = []
  const count = 100
  const large = new Array(1024).fill('a').join('')
  for (let i = 0; i < count; i++) {
    data.push(toBuf({ i, large }))
  }

  const begin = Date.now()
  for (const buf of data) {
    await log.append(buf)
  }

  const ms = Date.now() - begin
  console.log(`done ${count} in ${ms}ms`)

  const seconds = ms / 1000
  let avg = (count / seconds).toFixed(1)
  console.log(`${avg} append per second`)

  avg = (avg / 1000).toFixed(2)
  console.log(`${avg} append per ms`)

  t.pass('ok')
  t.teardown(() => log.close())
  console.log(`\n`)
}

test('test append 100 large', (t) => testAppendLarge(t, new Encoder()))
test('test append 100 large - xxhash body', (t) => testAppendLarge(t, new XxHashEncoder()))
test('test append 100 large - xxhash no body', (t) => testAppendLarge(t, new XxHashEncoder(false)))
