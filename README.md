# tinyraftplus
This is [tinyraft](https://www.npmjs.com/package/tinyraft) with extras. As with all Raft implementations a leader is elected and the network can survive if any majority of nodes are online, what has been added to tinyraft is...

### Log replication
A log with append(), appendBatch(), and truncate()

### Replication groups
Nodes may be assigned groups to support for example majority replication in CloudA and CloudB

### BigInt sequence numbers
Uses JS native BigInt for sequence numbers so you can basically grow to infinity

## Usage
```js
const { RaftNode, FsLog } = require('tinyraftplus')

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
```

## Output
```
append 0 { a: 1 }
append 1 { b: 2 }
append 2 { c: 3 }
head { c: 3 }
head { c: 3 }
head { c: 3 }
```

## Configuration
+ [tinyraft](https://www.npmjs.com/package/tinyraft)
+ [raftnode](https://github.com/rhodey/tinyraftplus/blob/master/index.js#L17)
+ [fslog](https://github.com/rhodey/tinyraftplus/blob/master/index.js#L227)

## Tests
```
npm run test
```

## License
MIT
