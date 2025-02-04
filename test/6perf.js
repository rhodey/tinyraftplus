const test = require('tape')
const { FsLog } = require('../lib/fslog.js')

const toBuf = (obj) => {
  if (obj === null) { return null }
  obj = JSON.stringify(obj)
  return Buffer.from(obj, 'utf8')
}

test('test append 100 small', async (t) => {
  t.plan(1)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

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
  t.teardown(() => log.stop())
})

test('test append 100 large', async (t) => {
  t.plan(1)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

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
  t.teardown(() => log.stop())
})
