const { RaftNode, FsLog } = require('./src/index.js')

const toBuf = (obj) => Buffer.from(JSON.stringify(obj), 'utf8')
const toObj = (buf) => JSON.parse(buf.toString('utf8'))

const ids = new Array(3).fill(0).map((z, idx) => ++idx)
const nodes = ids.map((id) => node(id, ids))

function node(id, ids) {
  const send = (to, msg) => {
    const node = nodes.find((node) => node.id === to)
    node.onReceive(id, msg) // from, msg
  }
  const log = new FsLog('/tmp/', 'node'+id)
  const opts = { quorum: 3 } // full repl for demo
  return new RaftNode(id, ids, send, log, opts)
}

async function main() {
  await Promise.all(nodes.map((node) => node.log.del()))
  await Promise.all(nodes.map((node) => node.open()))
  console.log('open')
  await Promise.all(nodes.map((node) => node.awaitLeader(1)))
  console.log('have leader')

  // append to any node = fwd to leader
  let seq = await nodes[0].append(toBuf({ a: 1 }))
  console.log('seq =', seq)
  seq = await nodes[1].append(toBuf({ b: 2 }))
  console.log('seq =', seq)
  seq = await nodes[2].append(toBuf({ c: 3 }))
  console.log('seq =', seq)

  console.log('head', toObj(nodes[0].head))
  console.log('head', toObj(nodes[1].head))
  console.log('head', toObj(nodes[2].head))

  await Promise.all(nodes.map((node) => node.close()))
}

main().catch(console.log)
