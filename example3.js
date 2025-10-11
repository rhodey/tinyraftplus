const sodium = require('libsodium-wrappers')
const { RaftNode, FsLog, TimeoutLog } = require('./src/index.js')
const { EncryptingEncoder } = require('./src/index.js')
const { tcpServer, tcpClient } = require('./src/index.js')

function errCb(err) {
  console.error('error', err)
  process.exit(1)
}

function opts() {
  let myCount = 0n
  const apply = (bufs) => {
    const results = []
    bufs.forEach((buf) => results.push(buf ? ++myCount : null))
    return results
  }
  const read = () => myCount
  const groupFn = (nodes) => nodes.length >= 2
  return { apply, read, group: 'name', groupFn }
}

function node(sodium, key, id, ids) {
  const clients = {}
  const send = (to, msg) => {
    let client = clients[to]
    if (!client) {
      const [host, port] = to.split(`:`)
      client = clients[to] = tcpClient(sodium, key, host, parseInt(port)).then((sock) => sock)
    }
    return client.then((sock) => sock.write(msg))
  }
  const encoder = new EncryptingEncoder(sodium, key)
  let log = new FsLog('/tmp/', 'node'+id, { encoder })
  log = new TimeoutLog(log, { default: 1_000 })
  const node = new RaftNode(id, ids, send, log, opts)
  const port = parseInt(id.split(`:`)[1])
  const msgCb = (sock, msg) => node.onReceive(msg.from, msg)
  return tcpServer(sodium, key, port, msgCb, errCb).then((srv) => {
    node.clients = clients
    node.srv = srv
    return node
  })
}

async function main() {
  console.log('boot')
  await sodium.ready
  const key = sodium.crypto_generichash(32, sodium.from_string('key'))

  const ids = new Array(3).fill(0).map((z, idx) => `127.0.0.1:${9000 + idx + 1}`)
  let nodes = ids.map((id) => node(sodium, key, id, ids))
  nodes = await Promise.all(nodes)

  await Promise.all(nodes.map((node) => node.log.del()))
  await Promise.all(nodes.map((node) => node.open()))
  await Promise.all(nodes.map((node) => node.awaitLeader()))
  console.log('ready')

  const leader = nodes.find((node) => node.state === 'leader')
  const buf = Buffer.from(new Array(1024).fill('a').join(''), 'utf8')

  let bufs = []
  const producer = setInterval(() => {
    bufs.push(buf)
    bufs.push(buf)
    bufs.push(buf)
  }, 500)

  const consumer = setInterval(() => {
    const copy = [...bufs]
    bufs = []
    leader.appendBatch(copy).then((ok) => {
      const [seq, count] = ok
      console.log(seq, count)
    }).catch(errCb)
  }, 500)

  const end = () => {
    clearInterval(producer)
    clearInterval(consumer)
    Promise.all(nodes.map((node) => node.close())).then(() => {
      nodes.map((node) => Object.values(node.clients)).flat().forEach((client) => client.then((conn) => conn.destroy()))
      nodes.forEach((node) => node.srv.close())
    }).catch(errCb)
  }

  setTimeout(end, 10_000)
}

main().catch(errCb)
