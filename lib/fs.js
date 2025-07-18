const fs = require('fs')
const fsp = require('fs/promises')
const { mkdirp } = require('mkdirp')

// todo: BigInt

const openOrCreate = async (path, flag, again=true) => {
  const dir = path.split('/')
  dir.pop()
  await mkdirp(dir.join('/'))
    .catch((err) => { throw new Error(`${dir} mkdirp error - ${err.message}`) })
  return fsp.open(path, flag).then((fh) => {
    fh.name = path
    return fh
  }).catch((err) => {
    if (err.code === 'ENOENT' && again) {
      return fsp.writeFile(path, Buffer.allocUnsafe(0))
        .then(() => openOrCreate(path, flag, false))
    } else if (err.code === 'ENOENT') {
      throw new Error(`${path} create error - ${err.message}`)
    }
    throw new Error(`${path} open error - ${err.message}`)
  })
}

const read = (fh, buf, off, len, pos) => {
  return new Promise((res, rej) => {
    fs.read(fh.fd, buf, Number(off), Number(len), Number(pos), (err, bytesRead, buf) => {
      if (err) {
        rej(new Error(`${fh.name} read error - ${err.message}`) )
      } else {
        res({ bytesRead })
      }
    })
  })
}

const truncate = (fh, len) => {
  return fh.truncate(Number(len))
    .catch((err) => { throw new Error(`${fh.name} truncate error - ${err.message}`) })
}

const sync = async (arg) => {
  const fh = typeof arg === 'string' ? null : arg
  const path = typeof arg === 'string' ? arg : null

  if (fh) {
    return fh.sync()
      .catch((err) => { throw new Error(`${fh.name} sync fh error - ${err.message}` )})
  }

  let fd = null
  try {
    // not speed critical path
    fd = fs.openSync(path, 'r')
    fs.fsyncSync(fd)
  } catch (err) {
    throw new Error(`${path} sync fd error - ${err.message}`)
  } finally {
    if (!fd) { return }
    fs.closeSync(fd)
  }
}

const close = async (fh) => {
  if (fh === null) { return false }
  return fh.close().then(() => true).catch((err) => {
    if (err.code === 'ENOENT') { return true }
    throw new Error(`${fh.name} close error - ${err.message}`)
  })
}

const del = (path) => {
  return fsp.rm(path, { force: true })
    .catch((err) => { throw new Error(`${path} del error - ${err.message}`) })
}

module.exports = {
  openOrCreate,
  read,
  truncate,
  sync,
  close,
  del
}
