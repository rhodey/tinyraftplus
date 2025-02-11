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

async function acceptMsg(request, response) {
  if (!node) {
    response.writeHead(503)
    response.end('try again soon')
    return
  }
  const msg = await readBody(request)
  if (msg.seq !== undefined) { msg.seq = BigInt(msg.seq) }
  if (msg.data) { msg.data = Buffer.from(msg.data, 'base64') }
  const from = msg.from
  delete msg.from
  node.onReceive(from, msg)
  response.writeHead(200)
  response.end('ok')
}

async function health(request, response) {
  response.writeHead(200)
  response.end('ok')
}

async function handleHttp(request, response) {
  const path = request.url.split('?')[0]
  if (path.startsWith('/health')) {
    await health(request, response)
  } else if (path.startsWith('/peer')) {
    await acceptMsg(request, response)
  } else {
    on400(response)
  }
}

async function send(to, msg) {
  if (msg.seq !== undefined) { msg.seq = msg.seq.toString() }
  if (msg.data) { msg.data = msg.data.toString('base64') }
  msg.from = name
  msg = JSON.stringify(msg)
  const opts = { method: 'POST', hostname: to, port: 9000, path: '/peer' }
  await util.sendHttp(opts, msg)
    .catch((err) => console.log(`${name} send to ${to} error`, err.message))
}

const toBuf = (obj) => Buffer.from(JSON.stringify(obj), 'utf8')
const toObj = (buf) => JSON.parse(buf ? buf.toString('utf8') : 'null')

let node = null

async function boot() {
  console.log(name, 'booting', nodes)
  let log = new FsLog('/tmp/', 'log')
  log = new ConcurrentLog(log)
  log = new AutoRestartLog(log, onError)
  node = new RaftNode(name, nodes, send, log)
  await node.start()
  console.log(name, 'started')
  await node.awaitLeader()
  console.log(name, 'have leader', node.leader)

  let c = 1
  setInterval(() => {
    node.append(toBuf({ count: c++ }))
      .then(() => console.log(name, 'head', node.log.seq, toObj(node.log.head)))
      .catch(onError)
  }, 2000)
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

const server = http.createServer(handle)
server.listen(port)

boot(nodes, name)
  .catch(console.log)
