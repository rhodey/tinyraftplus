const crypto = require('crypto')
const sodium = require('libsodium-wrappers')

const noop = () => {}

function onError(err) {
  console.log('error', err)
  process.exit(1)
}

function on500(req, err) {
  console.log('500', req.url, err)
  return new Response('500', { status: 500 })
}

function on400(req) {
  return new Response('400', { status: 400 })
}

function paramsOfPath(url) {
  const query = url.split('?')[1]
  return Object.fromEntries(new URLSearchParams(query))
}

const callbacks = {}

function receive(msg) {
  if (msg.type !== 'ack') { return }
  const cbs = callbacks[msg.cid]
  if (!cbs) { return }
  const [res, rej] = cbs
  delete callbacks[msg.cid]
  if (msg.err) { return rej(new Error(msg.err)) }
  res(msg)
}

const nodes = {}

function connect(leader) {
  const node = nodes[leader]
  if (node) { return node }
  const [host, port] = leader.split(':')
  return nodes[leader] = tcpClient(host, parseInt(port), receive).then((sock) => {
    const errCb = (err) => {
      console.log(`${leader} connection err`, err.message)
      nodes[leader] = undefined
      sock.destroy()
    }
    nodes[leader] = sock
    console.log(`${leader} connection open`)
    sock.on('error', errCb)
    sock.once('close', () => errCb(new Error('close')))
    return sock
  }).catch((err) => {
    console.log(`${leader} connection fail`)
    nodes[leader] = undefined
    throw err
  })
}

function send(leader, msg) {
  msg.cid = crypto.randomUUID()
  return new Promise((res, rej) => {
    callbacks[msg.cid] = [res, rej]
    const sock = connect(leader)
    if (!(sock instanceof Promise)) { return sock.write(msg) }
    sock.then((sock) => sock.write(msg)).catch(rej)
  }).catch((err) => {
    delete callbacks[msg.cid]
    throw err
  })
}

const bump = (leader) => {
  let [host, port] = leader.split(':')
  port = parseInt(port) + 1
  return `${host}:${port}`
}

// todo: if all nodes die term will reset
function updateShard(shard, leader, term) {
  leader = bump(leader)
  const prev = shards[shard]
  if (!prev?.leader) {
    shards[shard] = { leader, term }
    connect(leader).catch(noop)
    return true
  } else if (prev.leader === leader) {
    prev.term = term
    return false
  }
  shards[shard] = { leader, term }
  connect(leader).catch(noop)
  return true
}

function acceptShardInfo(req) {
  const params = paramsOfPath(req.url)
  let { shard, leader, term } = params
  shard = parseInt(shard)
  term = parseInt(term)
  const ok = updateShard(shard, leader, term)
  if (ok) {
    console.log(`update shard = ${shard} ${leader} ${term}`)
    return new Response(`update shard = ${shard} ${leader} ${term}`)
  }
  console.log(`stale shard = ${shard} ${leader} ${term}`)
  return new Response('stale')
}

// todo: maybe something faster
// maybe: Bun.hash
const hash = (str) => str.split('').reduce((acc, s) => acc + s.charCodeAt(0), 1)

function acceptMsg(req) {
  const params = paramsOfPath(req.url)
  const shard = hash(params.user) % shards.length
  const leader = shards[shard].leader
  if (!leader) { return new Response('503', { status: 503 }) }
  const data = Buffer.from(params.text, 'utf8')
  return send(leader, { type: 'msg', data })
    .then((ok) => new Response(`ok ${ok.seq}`))
}

function acceptMsgBatch(req) {
  const params = paramsOfPath(req.url)
  const shard = hash(params.user) % shards.length
  const leader = shards[shard].leader
  if (!leader) { return new Response('503', { status: 503 }) }
  const data = Buffer.from(params.text, 'utf8')
  return send(leader, { type: 'batch', data })
    .then((ok) => new Response(`ok ${ok.seq}`))
}

function health(req) {
  return new Response('ok')
}

async function handleHttp(req) {
  const path = new URL(req.url).pathname
  if (path.startsWith('/health')) {
    return health(req)
  } else if (path.startsWith('/info')) {
    return acceptShardInfo(req)
  } else if (path.startsWith('/msg')) {
    return acceptMsg(req)
  } else if (path.startsWith('/batch')) {
    return acceptMsgBatch(req)
  }
  return on400(req)
}

const handle = (req) => {
  return handleHttp(req)
    .catch((err) => on500(req, err))
}

let shards = parseInt(process.env.shards)
shards = new Array(shards).fill(0).map((n, i) => {
  return { leader: null, term: -1 }
})

let key = null
let tcpClient = null

async function boot() {
  console.log('booting')
  await sodium.ready
  const pass = process.env.password
  key = sodium.crypto_generichash(32, sodium.from_string(pass))
  const tcp = require('./lib/tcp.js')(sodium)
  tcpClient = (host, port, msgCb) => tcp.tcpClient(host, port, key, msgCb)
  // todo: server timeouts
  Bun.serve({ port: 9300, fetch: handle })
  console.log('ready')
}

boot().catch(onError)
