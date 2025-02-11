const { RaftNode, FsLog } = require('./index.js')

const toBuf = (obj) => Buffer.from(JSON.stringify(obj), 'utf8')
const toObj = (buf) => JSON.parse(buf.toString('utf8'))

let nodes = []
for (let i = 1; i <= 3; i++) {
  nodes.push(i)
}

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
  nodes = nodes.map((id) => node(id, nodes))
  await Promise.all(nodes.map((node) => node.start()))
  await Promise.all(nodes.map((node) => node.awaitLeader()))

  let ok = await nodes[0].append(toBuf({ a: 1 }))
  console.log('append', ok.seq, toObj(ok.data))

  ok = await nodes[1].append(toBuf({ b: 2 }))
  console.log('append', ok.seq, toObj(ok.data))

  ok = await nodes[2].append(toBuf({ c: 3 }))
  console.log('append', ok.seq, toObj(ok.data))

  console.log('head', toObj(nodes[0].log.head))
  console.log('head', toObj(nodes[1].log.head))
  console.log('head', toObj(nodes[2].log.head))

  await Promise.all(nodes.map((node) => node.stop()))
}

main().catch(console.log)
