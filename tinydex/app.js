const net = require('net')
const minimist = require('minimist')
const sodium = require('libsodium-wrappers')
const { TcpLogClient, ConcurrentLog } = require('tinyraftplus')
const { RaftNode, EncryptingEncoder } = require('tinyraftplus')

const noop = () => {}

function onError(err) {
  console.log('error', err)
  process.exit(1)
}

function onWarn(err) {
  console.log('warn', err)
}

let batch = []

function receiveGateway(sock, msg) {
  if (msg.type === 'msg') {
    node.append(msg.data)
      .then((seq) => sock.write({ type: 'ack', cid: msg.cid, seq }))
      .catch((err) => sock.write({ type: 'ack', cid: msg.cid, err: err.message }))
      .catch((err) => sock.destroy())
  } else if (msg.type === 'batch') {
    new Promise((res, rej) => batch.push([res, rej, msg.data]))
      .then((seq) => sock.write({ type: 'ack', cid: msg.cid, seq }))
      .catch((err) => sock.write({ type: 'ack', cid: msg.cid, err: err.message }))
      .catch((err) => sock.destroy())
  } else {
    sock.destroy()
  }
}

function appendBatches() {
  setInterval(() => {
    const res = batch.map((arr) => arr[0])
    const rej = batch.map((arr) => arr[1])
    const data = batch.map((arr) => arr[2])
    if (data.length <= 0) { return }
    batch = []
    const begin = Date.now()
    node.appendBatch(data).then((seq) => {
      const diff = Date.now() - begin
      console.log(name, shard, 'batch time', seq, diff)
      res.forEach((cb) => cb(seq))
    }).catch((err) => rej.forEach((cb) => cb(err)))
  }, 100)
}

// todo: send regularly
function sendShardInfo(gateway, leader, term) {
  return fetch(`http://${gateway}/info?shard=${shard}&leader=${leader}&term=${term}`)
    .then((ok) => ok.text())
    .catch((err) => onWarn(err))
}

function watchHead() {
  setInterval(() => {
    const head = node.head ?? Buffer.from('null')
    console.log(name, shard, node.seq, head.toString())
  }, 2000)
  node.on('change', (state) => {
    if (state.state !== 'leader') { return }
    let followers = state.followers ?? []
    followers = followers.length - 1
    if (followers < node.minFollowers) { return }
    gateways.forEach((gateway) => sendShardInfo(gateway, node.nodeId, state.term))
  })
}

const peers = {}

function sendPeer(to, msg) {
  msg.from = name
  const peer = peers[to]
  if (peer instanceof Promise) { return }
  if (peer) { return peer.write(msg) }
  const [host, port] = to.split(':')
  peers[to] = tcpClient(host, parseInt(port)).then((sock) => {
    const errCb = (err) => {
      console.log(`${name} connection ${to} err`, err.message)
      peers[to] = undefined
      sock.destroy()
     }
    sock.on('error', errCb)
    sock.once('close', () => errCb(new Error('close')))
    console.log(`${name} connection ${to} open`)
    peers[to] = sock
  }).catch((err) => {
    console.log(`${name} connection ${to} fail`)
    peers[to] = undefined
  })
}

function receivePeer(sock, msg) {
  const from = msg.from
  delete msg.from
  node.onReceive(from, msg)
}

let node = null
let key = null
let tcpClient = null
let tcpServer = null

async function boot() {
  await sodium.ready
  const pass = process.env.password
  key = sodium.crypto_generichash(32, sodium.from_string(pass))
  const tcp = require('./lib/tcp.js')(sodium)
  tcpClient = (host, port) => tcp.tcpClient(host, port, key, noop)
  tcpServer = tcp.tcpServer
  console.log(name, shard, 'booting', nodes)

  const logArgs = () => ['/tmp/', 'remote']
  const path = logArgs().join('')
  let log = new TcpLogClient(host, 9000, path, logArgs)
  log = new ConcurrentLog(log)

  const encrypt = new EncryptingEncoder(sodium, key)
  const opts = { crypto: encrypt }
  node = new RaftNode(name, nodes, sendPeer, log, opts)

  await node.start()
  console.log(name, shard, 'started log')
  const port = parseInt(name.split(':')[1])
  await tcp.tcpServer(port, key, onError, receivePeer)
  console.log(name, shard, 'started peer socket')
}

async function startGateway() {
  const port = parseInt(name.split(':')[1]) + 1
  await tcpServer(port, key, onError, receiveGateway)
  console.log(name, shard, 'started gateway server')
}

const argv = minimist(process.argv.slice(2))
const shard = parseInt(argv._[0])
const host = argv._[1]
const name = argv._[2]

let gateways = process.env.gateways ?? ''
gateways = gateways.split(',')

let nodes = process.env['nodes_'+shard] ?? ''
nodes = nodes.split(',')
if (nodes.length < 3) { onError('need three or more nodes') }

boot().then(async () => {
  await node.awaitLeader()
  console.log(name, shard, 'have leader', node.leader)
  watchHead()
  appendBatches()
  startGateway()
  console.log(name, shard, 'ready')
}).catch(onError)
