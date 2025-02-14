const net = require('net')
const { PackrStream, UnpackrStream } = require('msgpackr')
const { getKey, EncryptStream, DecryptStream } = require('./sodium.js')

function tcpServer(sodium, key, port, errCb, msgCb) {
  const server = net.createServer((socket) => {
    socket.on('close', () => errCb(new Error(`${port} net close`)))
    socket.on('error', (err) => errCb(new Error(`${port} net error ${err.message}`)))
    const decrypt = new DecryptStream(sodium, key)
    decrypt.on('error', (err) => errCb(new Error(`${port} net decrypt error ${err.message}`)))
    const unpack = new UnpackrStream()
    unpack.on('error', (err) => errCb(new Error(`${port} net unpack error ${err.message}`)))
    socket.pipe(decrypt).pipe(unpack).on('data', msgCb)
  })
  server.on('error', (err) => errCb(new Error(`${port} net server error ${err.message}`)))
  return new Promise((res, rej) => {
    try {
      server.listen(port, '0.0.0.0', res)
    } catch (err) {
      rej(err)
    }
  })
}

function tcpClient(sodium, key, host, port, errCb) {
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
      const pack = new PackrStream()
      pack.on('error', (err) => errCb(new Error(`${host} ${port} net pack error ${err.message}`)))
      const encrypt = new EncryptStream(sodium, key)
      encrypt.on('error', (err) => errCb(new Error(`${host} ${port} net encrypt error ${err.message}`)))
      pack.pipe(encrypt).pipe(socket)
      res(pack)
    })
    socket.connect(port, host)
  })
}

module.exports = function init(sodium) {
  const server = (port, password, errCb, msgCb) => {
    const key = getKey(sodium, password)
    return tcpServer(sodium, key, port, errCb, msgCb)
  }
  const client = (host, port, password, errCb) => {
    const key = getKey(sodium, password)
    return tcpClient(sodium, key, host, port, errCb)
  }
  return { tcpServer: server, tcpClient: client }
}
