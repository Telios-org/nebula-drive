const fs = require('fs')
const EventEmitter = require('events')
const getDirName = require('path').dirname
const path = require('path')
const Database = require('./lib/database')
const Hyperbee = require('hyperbee')
const Hypercore = require('hypercore')
const pump = require('pump')
const Crypto = require('./lib/crypto')
const Swarm = require('./lib/swarm')
const stream = require('stream')
const blake = require('blakejs')
const Hyperswarm = require('hyperswarm')
const MemoryStream = require('memorystream')
const { v4: uuidv4 } = require('uuid')
const FixedChunker = require('./util/fixedChunker.js')
const RequestChunker = require('./util/requestChunker.js')
const WorkerKeyPairs = require('./util/workerKeyPairs.js')

const HASH_OUTPUT_LENGTH = 32 // bytes
const MAX_PLAINTEXT_BLOCK_SIZE = 65536
const MAX_ENCRYPTED_BLOCK_SIZE = 65553
const FILE_TIMEOUT = 10000 // How long to wait for the on data event when downloading a file from a remote drive.
const FILE_RETRY_ATTEMPTS = 3 // How many parallel requests are made in each file request batch
const FILE_BATCH_SIZE = 10


class Drive extends EventEmitter {
  constructor(drivePath, peerPubKey, { keyPair, writable, swarmOpts, encryptionKey, fileTimeout }) {
    super()

    this.encryptionKey = encryptionKey
    this.database = null;
    this.db = null;
    this.drivePath = drivePath
    this.swarmOpts = swarmOpts
    this.publicKey = null
    this.peerPubKey = peerPubKey // Key used to clone and seed drive. Should only be shared with trusted sources
    this.keyPair = keyPair // ed25519 keypair to listen on
    this.writable = writable
    this.fileTimeout = fileTimeout || FILE_TIMEOUT
    this.requestQueue = new RequestChunker(null, FILE_BATCH_SIZE)

    this._localCore = new Hypercore(path.join(drivePath, `./LocalCore`))
    this._swarm = null
    this._workerKeyPairs = new WorkerKeyPairs(FILE_BATCH_SIZE)
    this._collections = {}
    this._filesDir = path.join(drivePath, `./Files`)
    this._localHB = null // Local Key value datastore only. This db does not sync with remote drives.
    this._lastSeq = null

    if (!fs.existsSync(drivePath)) {
      fs.mkdirSync(drivePath)
    }

    if (!fs.existsSync(this._filesDir)) {
      fs.mkdirSync(this._filesDir)
    }

    this.requestQueue.on('process-queue', async files => {
      this.requestQueue.reset()

      await this.fetchFileBatch(files, (stream, file) => {
        return new Promise((resolve, reject) => {
          fs.mkdirSync(getDirName(this._filesDir + file.path), { recursive: true })

          const writeStream = fs.createWriteStream(this._filesDir + file.path)

          pump(stream, writeStream, (err) => {
            if (err) reject(err)

            setTimeout(() => {
              this.emit('file-sync', file)
            })

            resolve()
          })
        })
      })
    })

    process.on('uncaughtException', err => {
      // gracefully catch uncaught exceptions
    })
  }

  async ready() {
    await this._bootstrap()

    this.publicKey = this.database.localMetaCore.key.toString('hex')

    if (this.peerPubKey) {
      this.discoveryKey = createTopicHash(this.peerPubKey).toString('hex')
    } else {
      this.discoveryKey = createTopicHash(this.publicKey).toString('hex')
    }

    // Data here can only be read by peer drives
    // that are sharing the same drive secret
    this._collections.files = await this.database.collection('file')

    if (this.keyPair) {
      await this.connect()
    }

    this._lastSeq = await this._localHB.get('lastSeq')

    const stream = this.database.metaBase.createReadStream({ live: true })

    stream.on('data', async data => {
      const node = {
        ...JSON.parse(data.value.toString()),
        seq: data.seq
      }

      if (
        node.key !== '__peers' && !this._lastSeq ||
        node.key !== '__peers' && this._lastSeq && data.seq > this._lastSeq.seq
      ) {
        await this._update(node)
      }
    })

    // This stopped streaming async updates after migrating to autobase
    // const hs = this.metadb.createHistoryStream({ live: true, gte: this._lastSeq ? -1 : 1 })

    // hs.on('data', async data => {
    //   this.emit('sync', data)
    //   if (data.key !== '__peers') {
    //     data.value = JSON.parse(data.value).value
    //     await this._update(data)
    //   }
    // })

    // hs.on('error', err => {
    //   // catch get out of bounds errors
    // })

    this.opened = true
  }

  // Connect to the Hyperswarm network
  async connect() {
    if (this._swarm) {
      await this._swarm.close()
    }

    this._swarm = new Swarm({
      keyPair: this.keyPair,
      workerKeyPairs: this._workerKeyPairs.keyPairs,
      topic: this.discoveryKey,
      publicKey: this.peerPubKey || this.publicKey,
      isServer: this.swarmOpts.server,
      isClient: this.swarmOpts.client,
      acl: this.swarmOpts.acl
    })

    this._swarm.on('message', (peerPubKey, data) => {
      this.emit('message', peerPubKey, data)
    })

    this._swarm.on('file-requested', socket => {
      socket.once('data', async data => {
        const fileHash = data.toString('utf-8')
        const file = await this.metadb.get(fileHash)
      
        if (!file || file.value.deleted) {
          let err = new Error()
          err.message = 'Requested file was not found on drive'
          socket.destroy(err)
        } else {
          const readStream = fs.createReadStream(path.join(this.drivePath, `./Files${file.value.path}`))
          pump(readStream, socket, (err) => {
            // handle done
          })
        }
      })

      socket.on('error', (err) => {
        // handle errors
      })
    })

    await this._swarm.ready()
  }

  async addPeer(peerKey) {
    const remotePeers = await this._localHB.get('remotePeers')

    const peers = [...remotePeers.value, peerKey]

    await this._localHB.put('remotePeers', peers)

    await this.database.addInput(peerKey)
  }

  // Remove Peer
  async removePeer(peerKey) {
    await this.database.removeInput(peerKey)
  }

  async writeFile(path, readStream, opts = {}) {
    let filePath = path
    let dest
    const uuid = uuidv4()

    if (filePath[0] === '/') {
      filePath = filePath.slice(1, filePath.length)
    }

    if (opts.encrypted) {
      dest = `${this._filesDir}/${uuid}`
    } else {
      fs.mkdirSync(getDirName(this._filesDir + path), { recursive: true })
      dest = this._filesDir + path
    }

    return new Promise(async (resolve, reject) => {
      const pathSeg = filePath.split('/')
      let fullFile = pathSeg[pathSeg.length - 1]
      let fileName
      let fileExt

      if (fullFile.indexOf('.') > -1) {
        fileName = fullFile.split('.')[0]
        fileExt = fullFile.split('.')[1]
      }

      const writeStream = fs.createWriteStream(dest)

      if (opts.encrypted && !opts.skipEncryption) {
        const fixedChunker = new FixedChunker(readStream, MAX_PLAINTEXT_BLOCK_SIZE)
        const { key, header, file } = await Crypto.encryptStream(fixedChunker, writeStream)

        await this.metadb.put(file.hash, {
          uuid,
          size: file.size,
          hash: file.hash,
          path: `/${uuid}`,
          peer_key: this.keyPair.publicKey.toString('hex'),
          discovery_key: this.discoveryKey
        })

        const fileMeta = {
          uuid,
          name: fileName,
          size: file.size,
          mimetype: fileExt,
          encrypted: true,
          key: key.toString('hex'),
          header: header.toString('hex'),
          hash: file.hash,
          path: filePath,
          peer_key: this.keyPair.publicKey.toString('hex'),
          discovery_key: this.discoveryKey
        }

        await this._collections.files.put(filePath, fileMeta)

        this.emit('file-add', fileMeta)

        resolve({
          key: key.toString('hex'),
          header: header.toString('hex'),
          ...fileMeta
        })
      } else {
        let bytes = ''
        const hash = blake.blake2bInit(HASH_OUTPUT_LENGTH, null)
        const calcHash = new stream.Transform({
          transform
        })

        function transform(chunk, encoding, callback) {
          bytes += chunk.byteLength

          blake.blake2bUpdate(hash, chunk)
          callback(null, chunk)
        }

        pump(readStream, calcHash, writeStream, async () => {
          setTimeout(async () => {
            const _hash = Buffer.from(blake.blake2bFinal(hash)).toString('hex')

            if (bytes > 0) {
              await this.metadb.put(_hash, {
                uuid,
                size: bytes,
                hash: _hash,
                path,
                peer_key: this.keyPair.publicKey.toString('hex'),
                discovery_key: this.discoveryKey
              })

              const fileMeta = {
                uuid,
                name: fileName,
                size: bytes,
                mimetype: fileExt,
                hash: _hash,
                path: filePath,
                peer_key: this.keyPair.publicKey.toString('hex'),
                discovery_key: this.discoveryKey
              }

              await this._collections.files.put(filePath, fileMeta)

              this.emit('file-add', fileMeta)
              resolve(fileMeta)

            } else {
              reject('No bytes were written.')
            }
          })
        })
      }
    })
  }

  async readFile(path) {
    let file
    let filePath = path

    if (filePath[0] === '/') {
      filePath = filePath.slice(1, filePath.length)
    }

    try {
      file = await this._collections.files.get(filePath)

      file = file.value

      const stream = fs.createReadStream(`${this._filesDir}/${file.uuid}`)

      // If key then decipher file
      if (file.encrypted && file.key && file.header) {
        const fixedChunker = new FixedChunker(stream, MAX_ENCRYPTED_BLOCK_SIZE)
        return Crypto.decryptStream(fixedChunker, file.key, file.header)
      } else {
        return stream
      }
    } catch (err) {
      throw err
    }
  }

  decryptFileStream(stream, key, header) {
    const fixedChunker = new FixedChunker(stream, MAX_ENCRYPTED_BLOCK_SIZE)
    return Crypto.decryptStream(fixedChunker, key, header)
  }

  // TODO: Implement this
  fetchFileByHash(fileHash) {
  }

  fetchFileByDriveHash(discoveryKey, fileHash, opts = {}) {
    const keyPair = opts.keyPair || this.keyPair
    const memStream = new MemoryStream()
    const topic = blake.blake2bHex(discoveryKey, null, HASH_OUTPUT_LENGTH)


    if (!fileHash || typeof fileHash !== 'string') {
      return reject('File hash is required before making a request.')
    }

    if (!discoveryKey || typeof discoveryKey !== 'string') {
      return reject('Discovery key cannot be null and must be a string.')
    }

    this._initFileSwarm(memStream, topic, fileHash, 0, { keyPair })

    if (opts.key && opts.header) {
      return this.decryptFileStream(memStream, opts.key, opts.header)
    }

    return memStream
  }

  async fetchFileBatch(files, cb) {
    const batches = new RequestChunker(files, FILE_BATCH_SIZE)

    for (let batch of batches) {
      const requests = []

      for (let file of batch) {
        requests.push(new Promise(async (resolve, reject) => {
          if (file.discovery_key) {
            const keyPair = this._workerKeyPairs.getKeyPair()
            const stream = this.fetchFileByDriveHash(file.discovery_key, file.hash, { key: file.key, header: file.header, keyPair })

            await cb(stream, file)

            resolve()
          } else {
            // TODO: Fetch files by hash
          }
        }))
      }

      await Promise.all(requests)
      this.requestQueue.queue = []
    }
  }

  async _initFileSwarm(stream, topic, fileHash, attempts, { keyPair }) {
    if (attempts === FILE_RETRY_ATTEMPTS) {
      const err = new Error('Unable to make a connection or receive data within the allotted time.')
      err.fileHash = fileHash
      this._workerKeyPairs.release(keyPair.publicKey.toString('hex'))
      stream.destroy(err)
    }

    const swarm = new Hyperswarm({ keyPair })

    let connected = false
    let receivedData = false
    let streamError = false

    swarm.join(Buffer.from(topic, 'hex'), { server: false, client: true })

    swarm.on('connection', async (socket, info) => {
      receivedData = false

      if (!connected) {
        connected = true

        // Tell the host drive which file we want
        socket.write(fileHash)

        socket.on('data', (data) => {
          stream.write(data)
          receivedData = true
        })

        socket.once('end', () => {
          if (receivedData) {
            this._workerKeyPairs.release(keyPair.publicKey.toString('hex'))
            stream.end()
            swarm.destroy()
          }
        })

        socket.once('error', (err) => {
          stream.destroy(err)
          streamError = true
        })
      }
    })

    setTimeout(async () => {
      if (!connected || streamError || !receivedData && attempts < FILE_RETRY_ATTEMPTS) {
        attempts += 1
        await swarm.leave(topic)
        await swarm.destroy()

        this._initFileSwarm(stream, topic, fileHash, attempts, { keyPair })
      }
    }, this.fileTimeout)
  }

  async unlink(filePath) {
    let fp = filePath

    if (fp[0] === '/') {
      fp = filePath.slice(1, fp.length)
    }

    try {
      let file = await this._collections.files.get(fp)

      if (!file) return

      file = await this.metadb.get(file.value.hash)

      if(!file) return

      fs.unlinkSync(path.join(this._filesDir, file.value.path))

      await this._collections.files.put(fp, {
        uuid: file.value.uuid,
        deleted: true
      })

      await this.metadb.put(file.value.hash, {
        uuid: file.value.uuid,
        discovery_key: file.value.discovery_key,
        deleted: true
      })

      this.emit('file-unlink', file.value)
    } catch (err) {
      throw err
    }
  }

  async destroyHyperfile(path) {
    const filePath = await this.bee.get(path)
    const file = await this.bee.get(filePath.value.hash)
    await this._clearStorage(file.value)
  }

  async _bootstrap() {
    // Init local core
    this._localHB = new Hyperbee(this._localCore, {
      keyEncoding: 'utf-8',
      valueEncoding: 'json'
    })

    this.database = new Database(this.drivePath, {
      keyPair: this.keyPair,
      encryptionKey: this.encryptionKey,
      peerPubKey: this.peerPubKey,
      acl: this.swarmOpts.acl
    })

    await this.database.ready()

    this.db = this.database
    this.metadb = this.database.metadb
  }

  async _update(data) {

    let lastSeq
    lastSeq = await this._localHB.get(`lastSeq`)

    if (!lastSeq) lastSeq = { value: { seq: null } }

    if (
      data.type === 'put' &&
      !data.value.deleted &&
      data.value.peer_key !== this.keyPair.publicKey.toString('hex') &&
      lastSeq.value.seq !== data.seq
    ) {
      this.emit('sync')

      if (data.value.hash) {
        try {
          await this._localHB.put(`lastSeq`, { seq: data.seq })
          this.requestQueue.addFile(data.value)
        } catch (err) {
          throw err
        }
      }
    }

    if (
      data.type === 'put' &&
      data.value.deleted &&
      data.value.peer_key !== this.keyPair.publicKey.toString('hex')
    ) {
      try {
        const filePath = path.join(this._filesDir, `/${data.value.uuid}`)
        if (fs.existsSync(filePath)) {
          fs.unlinkSync(filePath)

          setTimeout(() => {
            this.emit('file-unlink', data.value)
          })
        }
      } catch (err) {
        throw err
      }
    }
  }

  info() {
    const bytes = getTotalSize(this.drivePath)
    return {
      size: bytes
    }
  }

  /**
   * Close drive and disconnect from all Hyperswarm topics
   */
  async close() {
    await this._swarm.close()
    await this.database.close()
    await this._localCore.close()

    this.openend = false
  }
}

function createTopicHash(topic) {
  const crypto = require('crypto')

  return crypto.createHash('sha256')
    .update(topic)
    .digest()
}

async function auditFile(stream, remoteHash) {
  return new Promise((resolve, reject) => {
    let hash = blake.blake2bInit(HASH_OUTPUT_LENGTH, null)

    stream.on('error', err => reject(err))
    stream.on('data', chunk => {
      blake.blake2bUpdate(hash, chunk)
    })
    stream.on('end', () => {
      const localHash = Buffer.from(blake.blake2bFinal(hash)).toString('hex')

      if (localHash === remoteHash)
        return resolve()

      reject('Hashes do not match')
    })
  })
}


const getAllFiles = function (dirPath, arrayOfFiles) {
  files = fs.readdirSync(dirPath)

  arrayOfFiles = arrayOfFiles || []

  files.forEach(function (file) {
    if (fs.statSync(dirPath + "/" + file).isDirectory()) {
      arrayOfFiles = getAllFiles(dirPath + "/" + file, arrayOfFiles)
    } else {
      arrayOfFiles.push(path.join(dirPath, file))
    }
  })

  return arrayOfFiles
}

const getTotalSize = function (directoryPath) {
  const arrayOfFiles = getAllFiles(directoryPath)

  let totalSize = 0

  arrayOfFiles.forEach(function (filePath) {
    totalSize += fs.statSync(filePath).size
  })

  return totalSize
}

module.exports = Drive
