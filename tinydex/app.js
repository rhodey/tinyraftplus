const http = require('http')
const minimist = require('minimist')
const { RaftNode, FsLog } = require('tinyraftplus')
const AutoRestartLog = require('./lib/restart.js')
const ConcurrentLog = require('./lib/concurrent.js')
const util = require('./lib/util.js')

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

function readBody(request) {
  return new Promise((res, rej) => {
    let str = ''
    request.on('data', (chunk) => str += chunk)
    request.on('error', rej)
    request.on('end', () => {
      try {
        res(JSON.parse(str))
      } catch (err) {
        rej(new Error('parse body failed'))
      }
    })
  })
}

async function acceptPeer(request, response) {
  const msg = await readBody(request)
  if (msg.seq !== undefined) { msg.seq = BigInt(msg.seq) }
  if (msg.data && !Array.isArray(msg.data)) { msg.data = Buffer.from(msg.data, 'base64') }
  if (Array.isArray(msg.data)) { msg.data = msg.data.map((str) => Buffer.from(str, 'base64')) }
  const from = msg.from
  delete msg.from
  node.onReceive(from, msg)
  response.writeHead(200)
  response.end('ok')
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

async function health(request, response) {
  response.writeHead(200)
  response.end(`ok ${name}`)
}

async function handleHttp(request, response) {
  const path = request.url.split('?')[0]
  if (path.startsWith('/health')) {
    await health(request, response)
  } else if (path.startsWith('/peer')) {
    await acceptPeer(request, response)
  } else if (path.startsWith('/msg')) {
    await acceptMsg(request, response)
  } else if (path.startsWith('/batch')) {
    await acceptMsgBatch(request, response)
  } else {
    on400(response)
  }
}

async function send(to, msg) {
  if (msg.seq !== undefined) { msg.seq = msg.seq.toString() }
  if (msg.data && !Array.isArray(msg.data)) { msg.data = msg.data.toString('base64') }
  if (Array.isArray(msg.data)) { msg.data = msg.data.map((buf) => buf.toString('base64')) }
  msg.from = name
  msg = JSON.stringify(msg)
  const opts = { method: 'POST', hostname: to, port: 9000, path: '/peer' }
  await util.sendHttp(opts, msg)
    .catch((err) => console.log(`${name} send to ${to} error`, err.message))
}

let node = null

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
}

async function boot() {
  console.log(name, 'booting', nodes)
  let log = new FsLog('/tmp/', 'log')
  log = new AutoRestartLog(log, onError)
  log = new ConcurrentLog(log)
  node = new RaftNode(name, nodes, send, log)
  await node.start()
  console.log(name, 'started')
}

const defaults = { port: 9000 }
const argv = minimist(process.argv.slice(2))
const opts = { ...defaults, ...argv }
const port = opts.port
const name = argv._[0]

let nodes = process.env.nodes ?? ''
nodes = nodes.split(',')
if (nodes.length < 3) { onError('need three or more nodes in env var') }

const handle = (request, response) => {
  handleHttp(request, response)
    .catch((err) => on500(request, response, err))
}

boot(nodes, name).then(async () => {
  const server = http.createServer(handle)
  server.listen(port)
  await node.awaitLeader()
  console.log(name, 'have leader', node.leader)
  watchHead()
  appendBatches()
}).catch(console.log)
