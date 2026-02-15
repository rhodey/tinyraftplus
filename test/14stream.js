const test = require('tape')
const sodium = require('libsodium-wrappers')
const { EncryptingStream, DecryptingStream } = require('../src/index.js')
const { sleep } = require('./util.js')

test('test encrypting stream', async (t) => {
  t.plan(3)
  await sodium.ready
  const key = sodium.crypto_generichash(32, sodium.from_string('test'))
  const encrypt = new EncryptingStream(key)
  const decrypt = new DecryptingStream(key)

  const input = ['one', 'two', 'three']

  encrypt.pipe(decrypt)
  const output = []

  decrypt.on('data', (buf) => {
    const i = output.length
    output.push(buf.toString('utf8'))
    t.equal(output[i], input[i], `output ${i} = input ${i}`)
  })

  input.forEach((str) => encrypt.write(Buffer.from(str, 'utf8')))
  await sleep(500)
})

test('test encrypting stream with big bufs', async (t) => {
  t.plan(9)
  await sodium.ready
  const key = sodium.crypto_generichash(32, sodium.from_string('test'))
  const encrypt = new EncryptingStream(key)
  const decrypt = new DecryptingStream(key)

  const input = []
  for (let i = 1; i <= 9; i++) {
    const str = new Array(i * 10 * 1024).fill(i + '').join('')
    const buf = Buffer.from(str, 'utf8')
    input.push(buf)
  }

  encrypt.pipe(decrypt)
  const output = []

  decrypt.on('data', (buf) => {
    const i = output.length
    output.push(buf)
    t.ok(output[i].equals(input[i]), `output ${i} = input ${i}`)
  })

  input.forEach((buf) => encrypt.write(buf))
  await sleep(500)
})
