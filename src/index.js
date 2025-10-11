const { FsLog } = require('./fslog.js')
const { MultiFsLog } = require('./multi.js')
const { TimeoutLog } = require('./timeout.js')
const { Encoder, XxHashEncoder, EncryptingEncoder } = require('./encoder.js')
const { EncryptingStream, DecryptingStream } = require('./stream.js')
const { tcpServer, tcpClient } = require('./tcp.js')
const { RaftNode } = require('./node.js')

const fatal = ['failed to abort', 'failed to reset', 'lock corrupt', 'meta corrupt', 'body corrupt', 'body decrypt error']

const isLogFatal = (err) => fatal.some((str) => err.message.includes(str))

module.exports = {
  FsLog, MultiFsLog, TimeoutLog, isLogFatal,
  Encoder, XxHashEncoder, EncryptingEncoder,
  EncryptingStream, DecryptingStream,
  tcpServer, tcpClient,
  RaftNode,
}
