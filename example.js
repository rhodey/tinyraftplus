const { RaftNode, FsLog } = require('./index.js')

const toBuf = (obj) => Buffer.from(JSON.stringify(obj), 'utf8')
const toObj = (buf) => JSON.parse(buf.toString('utf8'))

const ids = new Array(3).fill(0).map((z, idx) => ++idx)
const nodes = ids.map((id) => node(id, ids))

function node(id, ids) {
  const send = (to, msg) => {
    const node = nodes.find((node) => node.nodeId === to)
    node.onReceive(id, msg)
  }
  const log = new FsLog('/tmp/', 'node'+id)
  const opts = { minFollowers: 2 } // force full repl for demo
  return new RaftNode(id, ids, send, log, opts)
}

async function main() {
  await Promise.all(nodes.map((node) => node.open()))
  console.log('open')
  await Promise.all(nodes.map((node) => node.awaitLeader()))
  console.log('have leader')

  let seq = await nodes[0].append(toBuf({ a: 1 }))
  console.log('seq =', seq)
  seq = await nodes[1].append(toBuf({ b: 2 }))
  console.log('seq =', seq)
  seq = await nodes[2].append(toBuf({ c: 3 }))
  console.log('seq =', seq)

  console.log('head', toObj(nodes[0].log.head))
  console.log('head', toObj(nodes[1].log.head))
  console.log('head', toObj(nodes[2].log.head))

  await Promise.all(nodes.map((node) => node.close()))
}

main().catch(console.log)
