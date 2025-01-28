# tinyraftplus
This is [tinyraft](https://www.npmjs.com/package/tinyraft) with extras

## Features
### Log replication
An log with append(), appendBatch(), and remove()

### Hash chaining
Hash of previous log entry included in next

### Replication groups
Nodes may be assigned groups to support for example majority replication in CloudA and CloudB

## Test
```
npm run test
```

## License
MIT
