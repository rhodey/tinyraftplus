#!/bin/bash
set -e

mkdir -p node_modules/tinyraftplus
cp ../package.json node_modules/tinyraftplus
cp -r ../lib node_modules/tinyraftplus
cp ../index.js node_modules/tinyraftplus
sudo docker build -t tinydex:latest .
sudo docker build -f Dockerfile.host -t tinydex-host:latest .
