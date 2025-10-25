const { RaftNode, FsLog } = require('./src/index.js')

const toBuf = (obj) => Buffer.from(JSON.stringify(obj), 'utf8')
const toObj = (buf) => JSON.parse(buf.toString('utf8'))

const ids = new Array(3).fill(0).map((z, idx) => ++idx)
const nodes = ids.map((id) => node(id, ids))

function node(id, ids) {
  // opts may be fn
  const opts = () => {
    let myCount = 0n
    const apply = (bufs) => {
      const results = []
      bufs.forEach((buf) => results.push(buf ? ++myCount : null))
      return results
    }
    const read = (cmd) => myCount
    return { apply, read }
  }
  const send = (to, msg) => {
    const node = nodes.find((node) => node.id === to)
    node.onReceive(id, msg) // from, msg
  }
  const log = new FsLog('/tmp/', 'node'+id)
  return new RaftNode(id, ids, send, log, opts)
}

async function main() {
  await Promise.all(nodes.map((node) => node.open()))
  await Promise.all(nodes.map((node) => node.awaitLeader()))

  // return type has changed
  let ok = await nodes[0].append(toBuf({ a: 1 }))
  let [seq, result] = ok
  console.log('state', seq, result)

  ok = await nodes[1].append(toBuf({ b: 2 }))
  seq = ok[0]; result = ok[1]
  console.log('state', seq, result)

  ok = await nodes[2].append(toBuf({ c: 3 }))
  seq = ok[0]; result = ok[1]
  console.log('state', seq, result)

  // read from any node = fwd to leader
  const cmd = { any: 'type' }
  ok = await nodes[0].read(cmd)
  seq = ok[0]; result = ok[1]

  console.log('read ', seq, result)

  await Promise.all(nodes.map((node) => node.close()))
}

main().catch(console.log)
