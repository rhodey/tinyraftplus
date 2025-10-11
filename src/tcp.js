const net = require('net')
const { PackrStream, UnpackrStream } = require('msgpackr')
const { EncryptingStream, DecryptingStream } = require('./stream.js')

const noop = () => {}

function tcpServer(sodium, key, port, msgCb, errCb) {
  const server = net.createServer((sock) => {
    const decrypt = new DecryptingStream(sodium, key)
    const unpack = new UnpackrStream()
    const pack = new PackrStream()
    const encrypt = new EncryptingStream(sodium, key)
    const close = () => {
      decrypt.destroy()
      unpack.destroy()
      pack.destroy()
      encrypt.destroy()
      sock.destroy()
    }
    sock.on('error', close)
    decrypt.on('error', close)
    unpack.on('error', close)
    pack.on('error', close)
    encrypt.on('error', close)
    sock.once('close', close)
    pack.once('close', close)
    const onData = (data) => msgCb(pack, data)
    sock.pipe(decrypt).pipe(unpack).on('data', onData)
    pack.pipe(encrypt).pipe(sock)
  })
  server.on('error', (err) => errCb(new Error(`${port} net error ${err.message}`)))
  return new Promise((res, rej) => {
    try {
      server.listen(port, '0.0.0.0', () => res(server))
    } catch (err) {
      rej(err)
    }
  })
}

function tcpClient(sodium, key, host, port, msgCb=noop) {
  const sock = new net.Socket()
  return new Promise((res, rej) => {
    sock.on('error', (err) => rej(new Error(`net error ${err.message}`)))
    sock.once('close', (err) => rej(new Error(`close`)))
    sock.once('connect', () => {
      const pack = new PackrStream()
      const encrypt = new EncryptingStream(sodium, key)
      const decrypt = new DecryptingStream(sodium, key)
      const unpack = new UnpackrStream()
      const close = () => {
        pack.destroy()
        encrypt.destroy()
        decrypt.destroy()
        unpack.destroy()
        sock.destroy()
      }
      const emit = (err) => pack.emit('error', err)
      sock.on('error', (err) => emit(new Error(`net error ${err.message}`)))
      encrypt.on('error', (err) => emit(new Error(`encrypt error ${err.message}`)))
      decrypt.on('error', (err) => emit(new Error(`decrypt error ${err.message}`)))
      unpack.on('error', (err) => emit(new Error(`unpack error ${err.message}`)))
      pack.once('close', close)
      sock.once('close', close)
      pack.pipe(encrypt).pipe(sock)
      sock.pipe(decrypt).pipe(unpack).on('data', msgCb)
      res(pack)
    })
    sock.connect(port, host)
  })
}

module.exports = { tcpServer, tcpClient }
