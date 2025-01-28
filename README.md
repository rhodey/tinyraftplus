# tinyraftplus
This is [tinyraft](https://www.npmjs.com/package/tinyraft) with extras. As with all Raft implementations a leader is elected and the network can survive 49% of nodes failing, what has been added to tinyraft is...

### Log replication
A log with append(), appendBatch(), and remove()

### Replication groups
Nodes may be assigned groups to support for example majority replication in CloudA and CloudB

### Hash chaining
Hash of previous log entry included in next

### String sequence numbers
Uses [decimal.js](https://www.npmjs.com/package/decimal.js) to work with sequence numbers so will never overflow

## Usage
```js
const { TinyRaftPlus, TinyRaftLog } = require('tinyraftplus')

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
```

## Configuration
+ [tinyraft](https://www.npmjs.com/package/tinyraft) configs
+ [tinyraftplus](https://github.com/rhodey/tinyraftplus/blob/master/index.js#L17) configs
+ [sqlite based log](https://github.com/rhodey/tinyraftplus/blob/master/index.js#L227)

## Output
```
append 0 { a: 1 }
append 1 { b: 2 }
append 2 { c: 3 }
head { c: 3 }
head { c: 3 }
head { c: 3 }
```

## Test
```
npm run test
```

## License
MIT
