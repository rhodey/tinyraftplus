const test = require('tape')
const { FsLog } = require('../index.js')
const { AutoRestartLog, ConcurrentLog } = require('../index.js')
const { TcpLogServer, TcpLogClient } = require('../index.js')
const { Encoder, XxHashEncoder } = require('../index.js')

const stop = (arr) => Promise.all(arr.filter((s) => s).map((s) => s.stop()))

async function testRemote(t, encoder) {
  t.plan(14)
  t.teardown(() => stop([server, log]))

  const logFn = (args) => {
    const [dir, name] = args
    let log = new FsLog(dir, name, { encoder })
    log = new AutoRestartLog(log)
    return new ConcurrentLog(log)
  }

  const server = new TcpLogServer(9000, logFn)
  await server.start()
  t.ok(1, 'server start ok')

  const logArgs = () => ['/tmp/', 'remote']
  const path = logArgs().join('')
  const log = new TcpLogClient('127.0.0.1', 9000, path, logArgs)
  await log.del()
  await log.start()
  t.ok(1, 'log start ok')
  t.equal(log.seq, -1n, 'seq -1')
  t.equal(log.head, null, 'head null')

  let data = Buffer.from('abc123')
  let seq = await log.append(data)
  t.equal(seq, 0n, 'seq 0')
  t.equal(log.seq, 0n, 'seq 0')
  t.ok(data.equals(log.head), 'head ok')

  data = [Buffer.from('123'), Buffer.from('456')]
  seq = await log.appendBatch(data)
  t.equal(seq, 1n, 'seq 1')
  t.equal(log.seq, 2n, 'seq 2')
  t.ok(data[1].equals(log.head), 'head ok')

  await log.truncate(1n)
  t.equal(log.seq, 1n, 'seq 1')
  t.ok(data[0].equals(log.head), 'head ok')

  await log.stop()
  await log.start()
  t.equal(log.seq, 1n, 'seq 1')
  t.ok(data[0].equals(log.head), 'head ok')
}

test('test remote', (t) => testRemote(t, new Encoder()))
test('test remote - xxhash body', (t) => testRemote(t, new XxHashEncoder()))
test('test remote - xxhash no body', (t) => testRemote(t, new XxHashEncoder(false)))
