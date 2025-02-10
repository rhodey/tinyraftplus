FROM node:23-alpine

EXPOSE 9000

WORKDIR /app

COPY package.json /app
COPY package-lock.json /app
RUN npm install

COPY node_modules/tinyraftplus /app/node_modules/tinyraftplus

COPY ./lib /app/lib
COPY ./main.js /app

CMD ["npm", "run", "main"]
