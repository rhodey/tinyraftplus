# tinyraftplus
This is [tinyraft](https://www.npmjs.com/package/tinyraft) with extras

## Features
### Log replication
An append only log with append(), appendBatch(), and remove()

### Hash chaining
Hash of previous log entry included in next

### Replication groups
Nodes may be assigned groups to support for example majority replication in CloudA and CloudB

## Todo
+ node groups
+ restore via repl
+ rpc retry ends on change nodes
+ gcp restore = new network id

## Test
```
npm run test
```

## License
MIT
