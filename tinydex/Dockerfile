FROM oven/bun:alpine

EXPOSE 9100
EXPOSE 9200

WORKDIR /app

COPY package.json /app
COPY package-lock.json /app
RUN bun install

COPY node_modules/tinyraftplus /app/node_modules/tinyraftplus

COPY ./lib /app/lib
COPY ./app.js /app

COPY entry.sh /app/entry.sh
RUN chmod +x entry.sh

ENTRYPOINT ["/app/entry.sh"]
