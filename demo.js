const { TinyRaftPlus, TinyRaftLog } = require('./index.js')

const sleep = (ms) => new Promise((res, rej) => setTimeout(res, ms))

let nodes = []
for (let i = 1; i <= 3; i++) {
  nodes.push(i)
}

function node(id, ids) {
  const opts = {}
  const log = new TinyRaftLog(opts)
  const send = (to, msg) => {
    const node = nodes.find((node) => node.nodeId === to)
    node.onReceive(id, msg)
  }
  return new TinyRaftPlus(id, ids, send, log, opts)
}

async function main() {
  nodes = nodes.map((id) => node(id, nodes))
  await Promise.all(nodes.map((node) => node.start()))
  await sleep(100)

  const noHash = (data) => {
    data = { ...data }
    delete data.prev
    return data
  }

  let ok = await nodes[0].append({ a: 1 })
  console.log('append', ok.seq, noHash(ok.data))

  ok = await nodes[1].append({ b: 2 })
  console.log('append', ok.seq, noHash(ok.data))

  ok = await nodes[2].append({ c: 3 })
  console.log('append', ok.seq, noHash(ok.data))

  console.log('head', noHash(nodes[0].log.head))
  console.log('head', noHash(nodes[1].log.head))
  console.log('head', noHash(nodes[2].log.head))

  await Promise.all(nodes.map((node) => node.stop()))
}

main().catch(console.log)
