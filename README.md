# tinyraftplus
This is [tinyraft](https://www.npmjs.com/package/tinyraft) with extras

## Features
### Log replication
A log with append(), appendBatch(), and remove()

### Replication groups
Nodes may be assigned groups to support for example majority replication in CloudA and CloudB

### Hash chaining
Hash of previous log entry included in next

## Usage
```js
const { TinyRaftPlus, TinyRaftLog } = require('tinyraftplus')

const sleep = (ms) => new Promise((res, rej) => setTimeout(res, ms))

function start(id, ids, send) {
  const ops = {}
  const log = new TinyRaftLog(opts)
  const node = new TinyRaftPlus(id, ids, send, log, opts)
  return node.start().then(() => node)
}

let nodes = []
for (let i = 0; i < 3; i++) {
  nodes.push(i)
}

nodes = await nodes.map((id) => start(id, nodes, send))
await sleep(100)

let ok = await nodes[0].append({ a: 1 })
console.log(ok.seq, ok.data)

ok = await nodes[1].append({ b: 2 })
console.log(ok.seq, ok.data)

ok = await nodes[2].append({ c: 3 })
console.log(ok.seq, ok.data)

console.log(nodes[0].log.head)
console.log(nodes[1].log.head)
console.log(nodes[2].log.head)
```

## Test
```
npm run test
```

## License
MIT
