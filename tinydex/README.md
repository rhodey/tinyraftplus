# TinyDEX (decentralized exchange)
Example of what can be made with tinyraftplus

## Usage
```
./build.sh
cp example.env .env
docker compose up
npm install -g loadtest
loadtest --cores 3 --rps 10000 -t 15 http://localhost:8080/batch?text=abc123
loadtest --cores 4 --rps 20000 -t 15 http://localhost:8080/batch?text=abc123
```

## Test
```
npm run test
```

## License
MIT
