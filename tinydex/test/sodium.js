const test = require('tape')
const crypto = require('crypto')
const _sodium = require('libsodium-wrappers')
const { getKey, EncryptStream, DecryptStream } = require('..//lib/sodium.js')

const sleep = (ms) => new Promise((res, rej) => setTimeout(res, ms))

test('test sodium streams', async (t) => {
  await _sodium.ready
  sodium = _sodium

  const key = getKey(sodium, 'abc123')
  const e1 = new EncryptStream(sodium, key)
  const d1 = new DecryptStream(sodium, key)

  const datas1 = []
  for (let i = 0; i < 10_000; i++) {
    const buf = Buffer.from(crypto.randomUUID())
    datas1.push(buf)
  }

  let c1 = 0
  e1.pipe(d1).on('data', (data) => {
    const test = datas1[c1++]
    const ok = data.equals(test)
    if (ok) { return }
    console.log((c1-1), test.toString(), data.toString())
    throw new Error('problem 1')
  })

  const e2 = new EncryptStream(sodium, key)
  const d2 = new DecryptStream(sodium, key)

  const datas2 = []
  for (let i = 0; i < 10_000; i++) {
    const buf = Buffer.from(crypto.randomUUID())
    datas2.push(buf)
  }

  let c2 = 0
  e2.pipe(d2).on('data', (data) => {
    const test = datas2[c2++]
    const ok = data.equals(test)
    if (ok) { return }
    console.log((c2-1), test.toString(), data.toString())
    throw new Error('problem 2')
  })

  for (let i = 0; i < datas1.length; i++) {
    e1.write(datas1[i])
    e2.write(datas2[i])
  }

  t.ok(true, 'ok')
})
