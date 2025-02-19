const { FsLog, XxHashEncoder } = require('tinyraftplus')
const { AutoRestartLog, ConcurrentLog } = require('tinyraftplus')
const { TcpLogServer } = require('tinyraftplus')

function onError(err) {
  console.log('error', err)
  process.exit(1)
}

async function boot() {
  const logFn = (args) => {
    const [dir, name] = args
    const encoder = new XxHashEncoder()
    let log = new FsLog(dir, name, { encoder })
    log = new AutoRestartLog(log)
    return new ConcurrentLog(log)
  }
  const server = new TcpLogServer(9000, logFn)
  server.on('error', onError)
  server.on('close', () => onError(new Error('server closed')))
  await server.start()
  console.log('ready')
}

boot()
  .catch(console.log)
