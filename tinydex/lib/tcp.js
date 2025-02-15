const net = require('net')
const { PackrStream, UnpackrStream } = require('msgpackr')
const { EncryptStream, DecryptStream } = require('./streams.js')

// todo: survive client disconnect
function tcpServer(port, sodium, key, errCb, msgCb) {
  const server = net.createServer((socket) => {
    const decrypt = new DecryptStream(sodium, key)
    const unpack = new UnpackrStream()
    decrypt.on('error', (err) => errCb(new Error(`${port} net decrypt error ${err.message}`)))
    unpack.on('error', (err) => errCb(new Error(`${port} net unpack error ${err.message}`)))
    socket.on('error', (err) => errCb(new Error(`${port} net error ${err.message}`)))
    socket.on('close', () => errCb(new Error(`${port} net close`)))
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

// todo: survive server disconnect
function tcpClient(host, port, sodium, key, errCb) {
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
  const server = (port, key, errCb, msgCb) => tcpServer(port, sodium, key, errCb, msgCb)
  const client = (host, port, key, errCb) => tcpClient(host, port, sodium, key, errCb)
  return { tcpServer: server, tcpClient: client }
}
