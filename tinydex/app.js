const http = require('http')
const minimist = require('minimist')
const { RaftNode, FsLog } = require('tinyraftplus')

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

async function health(request, response) {
  response.writeHead(400)
  response.end('ok')
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
        rej(new Error('json parse http body failed'))
      }
    })
  })
}

async function peer(request, response) {
  const json = await readBody(request)
  json.data = Buffer.from(json.data, 'base64')
  response.writeHead(400)
  response.end('ok')
}

async function handleHttp(request, response) {
  const path = request.url.split('?')[0]
  if (path.startsWith('/health')) {
    await health(request, response)
  } else if (path.startsWith('/peer')) {
    await peer(request, response)
  } else {
    on400(response)
  }
}

async function sendToPeer(to, msg) {
  // todo:
}

async function boot(nodes, name) {
  console.log('booting', nodes, name)
  const log = new FsLog('/tmp/', 'log')
  const send = (to, msg) => sendToPeer(to, msg).catch(onError)
  const node = new RaftNode(name, nodes, send, log)
  await node.start()
  console.log('started')
  await node.awaitLeader()
  console.log('have leader')
}

const defaults = { port: 9000 }
const argv = minimist(process.argv.slice(2))
const opts = { ...defaults, ...argv }
const port = opts.port
const name = argv._[0]

const handle = (request, response) => {
  handleHttp(request, response)
    .catch((err) => on500(request, response, err))
}

const server = http.createServer(handle)
server.listen(port)

let nodes = process.env.nodes ?? ''
nodes = nodes.split(',')
if (nodes.length <= 2) { onError('need three or more nodes in env var') }

boot(nodes, name).catch(console.log)
