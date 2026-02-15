# TinyRaftPlus
This is the [TinyRaft](https://www.npmjs.com/package/tinyraft) API with extras. As with all Raft implementations a leader is elected and the network can survive if any majority of nodes are alive, what has been added to tinyraft is...

### Log replication
A log with append(), appendBatch(), txn(), commit(), abort(), trim(), iter()

### Replication groups
Nodes may be assigned groups to support for example majority replication in both CloudA, CloudB

### BigInt sequence numbers
Uses JS native BigInt for sequence numbers so you can grow to infinity

## Usage
```js
const { RaftNode, FsLog } = require('tinyraftplus')

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
```
```
open
have leader
seq = 1n
seq = 2n
seq = 3n
head { c: 3 }
head { c: 3 }
head { c: 3 }
```

## State Machine (optional)
```js
const { RaftNode, FsLog } = require('tinyraftplus')

const toBuf = (obj) => Buffer.from(JSON.stringify(obj), 'utf8')
const toObj = (buf) => JSON.parse(buf.toString('utf8'))

const ids = new Array(3).fill(0).map((z, idx) => ++idx)
const nodes = ids.map((id) => node(id, ids))

function node(id, ids) {
  // opts may be fn
  const opts = () => {
    let myCount = 0n
    const apply = (bufs, seq) => {
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
  await Promise.all(nodes.map((node) => node.log.del()))
  await Promise.all(nodes.map((node) => node.open()))
  await Promise.all(nodes.map((node) => node.awaitLeader(1)))

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
```
```
state 1n 1n
state 2n 2n
state 3n 3n
read  3n 3n
```

## You should know
The [Raft paper](https://raft.github.io/raft.pdf) explains that leaders must begin their term by appending a null buffer

With TinyRaftPlus seq begins with 0 but you see 1 in examples because of the null buffer

Your state machine apply function will encounter nulls and should return null for null so be aware

## RaftNode events
RaftNode emits change, commit, apply, warn, and error

Change is the same as [TinyRaft](https://www.npmjs.com/package/tinyraft), and commit and apply both emit a seq number

Warn (warn) emits an instance of Error and these errors are errors that replication avoids / recovers from

## Error events
Error (error) emits an instance of Error and these come from operations with the log, apply, or read

If your apply and read functions are not throwing errors then the log / fs is bad

If you suspect the fs restart the host. If node.open fails on restart you need to replace the node / host

Use [XxHashEncoder](https://github.com/rhodey/tinyraftplus/blob/master/src/encoder.js#L63) to identify fs corruption

## Configuration
+ [FsLog](https://github.com/rhodey/tinyraftplus/blob/master/src/fslog.js#L20)
+ [MultiFsLog](https://github.com/rhodey/tinyraftplus/blob/master/src/multi.js#L35)
+ [TimeoutLog](https://github.com/rhodey/tinyraftplus/blob/master/src/timeout.js#L19)
+ [XxHashEncoder](https://github.com/rhodey/tinyraftplus/blob/master/src/encoder.js#L63)
+ [RaftNode](https://github.com/rhodey/tinyraftplus/blob/master/src/node.js#L69)

## Performance
Node v20.11.0 (LTS) is best
+ FsLog append = 650 bufs/sec
+ FsLog append + txn = 50,000 bufs/sec
+ FsLog appendBatch = 100,000 bufs/sec
+ RaftNode append = 275 bufs/sec
+ RaftNode appendBatch = 100,000 bufs/sec

## Test
The [tests](https://github.com/rhodey/tinyraftplus/tree/master/test) include 3800+ assertions

The API is stable to dev against but I intend to add approx 25% more tests
```
npm run test
```

## Tcp
See [example3.js](https://github.com/rhodey/tinyraftplus/blob/master/example3.js) which shows a TCP example and advanced options

## License
mike@rhodey.org

MIT
