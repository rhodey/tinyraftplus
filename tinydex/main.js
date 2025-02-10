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

async function peer(request, response) {
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

async function boot(nodes) {
  console.log(123, nodes)
}

const defaults = { port: 9000 }
const argv = minimist(process.argv.slice(2))
const opts = { ...defaults, ...argv }
const port = opts.port

const handle = (request, response) => {
  handleHttp(request, response)
    .catch((err) => on500(request, response, err))
}

const server = http.createServer(handle)
server.listen(port)

let nodes = process.env.nodes ?? ''
nodes = nodes.split(',')
if (nodes.length <= 2) { onError('need three or more nodes in env var') }

boot(nodes).catch(console.log)
