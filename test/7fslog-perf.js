const test = require('tape')
const { FsLog } = require('../lib/fslog.js')

const toBuf = (obj) => {
  if (obj === null) { return null }
  obj = JSON.stringify(obj)
  return Buffer.from(obj, 'utf8')
}

test('test append many', async (t) => {
  t.plan(1)
  const log = new FsLog('/tmp/', 'test')
  await log.del()
  await log.start()

  const data = []
  const count = 100
  for (let i = 0; i < count; i++) {
    data.push(toBuf({ a: i }))
  }

  const begin = Date.now()
  for (const buf of data) {
    await log.append(buf)
  }

  const ms = Date.now() - begin
  console.log(`done ${count} in ${ms}ms`)

  const seconds = ms / 1000
  let avg = (count / seconds).toFixed(1)
  console.log(`${avg}/s`)

  avg = (avg / 1000).toFixed(2)
  console.log(`${avg}/ms`)

  t.pass('ok')
  t.teardown(() => log.stop())
})
