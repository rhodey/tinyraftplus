const net = require('net')
const http = require('http')
const { UnpackrStream } = require('msgpackr')
const { DecryptStream } = require('./sodium.js')

const httpTimeout = 5_000

const noop = () => { }
const sleep = (ms) => new Promise((res, rej) => setTimeout(res, ms))
const serialize = (params) => new URLSearchParams(params).toString()

// round timers forward to nearest 100ms
const error = new Error('timedout')
function timeout(ms) {
  let timer = null
  const timedout = new Promise((res, rej) => {
    const now = Date.now()
    let next = now + ms
    next = next - (next % 100)
    next = (100 + next) - now
    timer = setTimeout(rej, next, error)
  })
  return [timer, timedout]
}

function readBody(response) {
  return new Promise((res, rej) => {
    let str = ''
    response.setEncoding('utf8')
    response.on('error', rej)
    response.on('data', (chunk) => str += chunk)
    response.on('end', () => res(str))
  })
}

function sendHttp(options, body = '') {
  const path = options.path.split('?')[0]
  const info = `${options.method} ${options.hostname} ${path}`
  const [timer, timedout] = timeout(httpTimeout)

  options.family = 4
  if (!options.headers) { options.headers = {} }
  options.headers['Accept'] = 'application/json'

  if (body) {
    options.headers['Content-Type'] = options.headers['Content-Type'] ?? 'application/json'
    options.headers['Content-Length'] = body.byteLength
  }

  const result = new Promise((res, rej) => {
    timedout.catch((err) => rej(new Error(`http timeout ${info}`)))
    const request = http.request(options, (response) => {
      const { statusCode: code } = response
      if (code < 200 || code >= 300) {
        readBody(response)
          .then((str) => rej(new Error(`http code ${code} ${info} ${str}`)))
          .catch((err) => rej(new Error(`http code ${code} ${info}`)))
        return
      }
      readBody(response)
        .then(res)
        .catch((err) => rej(new Error(`http io error ${info} ${err.message}`)))
    })
    request.once('error', (err) => rej(new Error(`http error ${info} ${err.message}`)))
    request.end(body)
  })
  result.catch(noop).finally(() => clearTimeout(timer))
  return result
}

function tcpServer(port, sodium, key, errCb, msgCb) {
  const server = net.createServer((socket) => {
    socket.on('close', () => errCb(new Error(`net close`)))
    socket.on('error', (err) => errCb(new Error(`net error ${err.message}`)))
    const decrypt = new DecryptStream(sodium, key)
    const unpack = new UnpackrStream()
    decrypt.on('error', (err) => errCb(new Error(`net decrypt error ${err.message}`)))
    unpack.on('error', (err) => errCb(new Error(`net unpack error ${err.message}`)))
    socket.pipe(decrypt).pipe(unpack).on('data', msgCb)
  })
  server.on('error', (err) => errCb(new Error(`net server error ${err.message}`)))
  return new Promise((res, rej) => {
    try {
      server.listen(port, '0.0.0.0', res)
    } catch (err) {
      rej(err)
    }
  })
}

function tcpClient(host, port, errCb) {
  let connected = false
  const socket = new net.Socket()
  return new Promise((res, rej) => {
    socket.on('error', (err) => {
      err = new Error(`${host} ${port} net error ${err.message}`)
      if (connected) { errCb(err) }
      rej(err)
    })
    socket.on('close', () => {
      const err = new Error(`${host} ${port} net close`)
      if (connected) { errCb(err) }
      rej(err)
    })
    socket.once('connect', () => {
      connected = true
      res(socket)
    })
    socket.connect(port, host)
  })
}

module.exports = {
  sleep, timeout,
  serialize, sendHttp,
  tcpServer,
  tcpClient,
}
