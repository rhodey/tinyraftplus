const net = require('net')
const http = require('http')
const minimist = require('minimist')
const _sodium = require('libsodium-wrappers')
const { RaftNode, FsLog } = require('tinyraftplus')
const { AutoRestartLog, ConcurrentLog } = require('tinyraftplus')

function onError(err) {
  console.log('error', err)
  process.exit(1)
}

function on500(request, response, err) {
  console.error('500', request.url, err)
  response.writeHead(500)
  response.end()
}

function on400(response) {
  response.writeHead(400)
  response.end()
}

function paramsOfPath(path) {
  const query = path.split('?')[1]
  return Object.fromEntries(new URLSearchParams(query))
}

async function acceptMsg(request, response) {
  const params = paramsOfPath(request.url)
  const seq = await node.append(Buffer.from(params.text))
  response.writeHead(200)
  response.end(`ok ${seq}`)
}

let batch = []

async function acceptMsgBatch(request, response) {
  const params = paramsOfPath(request.url)
  const cb = (err, seq) => {
    if (err) {
      on500(request, response, err)
      return
    }
    response.writeHead(200)
    response.end(`ok ${seq}`)
  }
  const data = Buffer.from(params.text)
  batch.push([cb, data])
}

function appendBatches() {
  setInterval(() => {
    const cbs = batch.map((arr) => arr[0])
    const data = batch.map((arr) => arr[1])
    if (data.length <= 0) { return }
    batch = []
    const begin = Date.now()
    node.appendBatch(data).then((seq) => {
      cbs.forEach((cb) => cb(null, seq))
      const diff = Date.now() - begin
      console.log(name, 'batch time', seq, diff)
    }).catch(onError)
  }, 100)
}

function watchHead() {
  setInterval(() => {
    const head = node.log.head ?? Buffer.from('null')
    console.log(name, node.log.seq, head.toString())
  }, 2000)
  // todo: remove
  node.on('change', (st) => {
    console.log(
        '==== CHANGE '+node.nodeId+': '+(st.state == 'follower' ? 'following '+st.leader : st.state)+
        ', term '+st.term+(st.state == 'leader' ? ', followers: '+st.followers.join(', ') : '')
    )
  })
}

async function health(request, response) {
  response.writeHead(200)
  response.end(`ok ${name}`)
}

async function handleHttp(request, response) {
  const path = request.url.split('?')[0]
  if (path.startsWith('/health')) {
    await health(request, response)
  } else if (path.startsWith('/msg')) {
    await acceptMsg(request, response)
  } else if (path.startsWith('/batch')) {
    await acceptMsgBatch(request, response)
  } else {
    on400(response)
  }
}

const peers = {}

function send(to, msg) {
  let peer = peers[to]
  if (!peer) {
    peer = peers[to] = tcpClient(to, 9000, pass, onError).then((socket) => {
      console.log(`${name} connection to ${to} open`)
      peers[to] = socket
    }).catch((err) => {
      console.log(`${name} connection to ${to} failed`)
      peers[to] = undefined
    })
  }
  if (peer instanceof Promise) { return }
  msg.from = name
  peer.write(msg)
}

function receive(msg) {
  const from = msg.from
  delete msg.from
  node.onReceive(from, msg)
}

let node = null
let sodium = null
let tcpClient = null
const pass = process.env.password

async function boot() {
  await _sodium.ready
  sodium = _sodium
  const tcp = require('./lib/tcp.js')(sodium)
  tcpClient = tcp.tcpClient
  console.log(name, 'booting', nodes)
  let log = new FsLog('/tmp/', 'log')
  log = new AutoRestartLog(log, onError)
  log = new ConcurrentLog(log)
  node = new RaftNode(name, nodes, send, log)
  node.setMaxListeners(1024)
  await node.start()
  console.log(name, 'started log')
  await tcp.tcpServer(9000, pass, onError, receive)
  console.log(name, 'started peer socket')
}

function startHttp() {
  const handle = (request, response) => {
    handleHttp(request, response)
      .catch((err) => on500(request, response, err))
  }
  const httpServer = http.createServer(handle)
  httpServer.listen(9100)
  console.log(name, 'started http server')
}

const argv = minimist(process.argv.slice(2))
const name = argv._[0]

let nodes = process.env.nodes ?? ''
nodes = nodes.split(',')
if (nodes.length < 3) { onError('need three or more nodes in env var') }

boot().then(async () => {
  await node.awaitLeader()
  console.log(name, 'have leader', node.leader)
  watchHead()
  appendBatches()
  startHttp()
  console.log(name, 'ready')
}).catch(console.log)
