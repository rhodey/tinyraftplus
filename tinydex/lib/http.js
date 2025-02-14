const http = require('http')

const noop = () => { }
const sleep = (ms) => new Promise((res, rej) => setTimeout(res, ms))
const serialize = (params) => new URLSearchParams(params).toString()

const httpTimeout = 10_000

// round timers forward to nearest 100ms
const error = new Error('timedout')
function timeout(ms) {
  let timer = null
  const timedout = new Promise((res, rej) => {
    const now = Date.now()
    let next = now + ms
    next = next - (next % 100)
    next = (100 + next) - now
    timer = setTimeout(rej, next, error)
  })
  return [timer, timedout]
}

function readBody(response) {
  return new Promise((res, rej) => {
    let str = ''
    response.setEncoding('utf8')
    response.on('error', rej)
    response.on('data', (chunk) => str += chunk)
    response.on('end', () => res(str))
  })
}

function sendHttp(options, body = '') {
  const path = options.path.split('?')[0]
  const info = `${options.method} ${options.hostname} ${path}`
  const [timer, timedout] = timeout(httpTimeout)

  options.family = 4
  if (!options.headers) { options.headers = {} }
  options.headers['Accept'] = 'application/json'

  if (body) {
    options.headers['Content-Type'] = options.headers['Content-Type'] ?? 'application/json'
    options.headers['Content-Length'] = body.byteLength
  }

  const result = new Promise((res, rej) => {
    timedout.catch((err) => rej(new Error(`http timeout ${info}`)))
    const request = http.request(options, (response) => {
      const { statusCode: code } = response
      if (code < 200 || code >= 300) {
        readBody(response)
          .then((str) => rej(new Error(`http code ${code} ${info} ${str}`)))
          .catch((err) => rej(new Error(`http code ${code} ${info}`)))
        return
      }
      readBody(response)
        .then(res)
        .catch((err) => rej(new Error(`http io error ${info} ${err.message}`)))
    })
    request.once('error', (err) => rej(new Error(`http error ${info} ${err.message}`)))
    request.end(body)
  })
  result.catch(noop).finally(() => clearTimeout(timer))
  return result
}

module.exports = { sleep, timeout, serialize, sendHttp }
