# TinyDEX (decentralized exchange)
Example of what can be made with tinyraftplus

## Usage
```
./build.sh
cp example.env .env
docker compose up
npm install -g loadtest
loadtest --rps 1000 -t 15 'http://localhost:9300/batch?user=abcde&text=shard0000'
loadtest --rps 1000 -t 15 'http://localhost:9300/batch?user=abc&text=shard1111'
```

## Test
```
npm run test
```

## License
MIT
