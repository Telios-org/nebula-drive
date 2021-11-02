const Autobase = require('autobase')
const Hyperbee = require('hyperbee')
const Hypercore = require('hypercore')
const EventEmitter = require('events')
const Autobee = require('./autobee')
const Hyperswarm = require('hyperswarm')
const pump = require('pump')

class Database extends EventEmitter {
  constructor(storage, opts = {}) {
    super()

    const key = opts.peerPubKey ? opts.peerPubKey : null;
    
    this.keyPair = opts.keyPair
    this.acl = opts.acl
    this.peerPubKey = key
    this.encryptionKey = opts.encryptionKey
    this.storage = typeof storage === 'string' ? `${storage}/Database` : storage
    
    // Init local Autobase
    this.localWriter = new Hypercore(`${this.storage}/local-writer`, null, { encryptionKey: this.encryptionKey })
    this.autobaseIndex = new Hypercore(`${this.storage}/local-index`, null, { encryptionKey: this.encryptionKey })
    this.base = new Autobase([this.localWriter], {
      input: this.localWriter,
      indexes: this.autobaseIndex
    })
    this.bee = new Autobee(this.base, {
      keyEncoding: 'utf-8',
      valueEncoding: 'json'
    })

    // Init Meta Autobase
    this.localMetaCore = new Hypercore(`${this.storage}/meta-local`)
    this.remoteMetaCore = new Hypercore(`${this.storage}/meta-remote`, this.peerPubKey)
    this.metaIndex = new Hypercore(`${this.storage}/meta-index`, null)
    this.metaBase = new Autobase([this.localMetaCore], {
      input: this.localMetaCore,
      indexes: this.metaIndex
    })
    this.metadb = new Autobee(this.metaBase, {
      keyEncoding: 'utf-8',
      valueEncoding: 'json'
    })

    this.collections = {}
    this.cores = [
      this.localWriter,
      this.autobaseIndex,
      this.localMetaCore,
      this.remoteMetaCore,
      this.metaIndex
    ]
    this.connections = []
  }

  async ready() {
    await this._joinSwarm(this.localWriter, { server: true, client: false })
    await this._joinSwarm(this.localMetaCore, { server: true, client: false })

    let remotePeers

    if(this.peerPubKey) {
      await this._joinSwarm(this.remoteMetaCore, { server: false, client: true })
      await this.metaBase.addInput(this.remoteMetaCore)
      
      remotePeers = await this.metadb.get('__peers')
    } else {
      await this._joinSwarm(this.remoteMetaCore, { server: true, client: false })
    }
    
    if(!remotePeers) {
      await this.metadb.put('__peers', {
        [this.keyPair.publicKey.toString('hex')]: {
          writer: this.localWriter.key.toString('hex'),
          meta: this.localMetaCore.key.toString('hex')
        }
      })
    } else {
      for (const key in remotePeers.value) {
        const peer = remotePeers.value[key]

        const peerWriter = new Hypercore(`${this.storage}/peers/${peer.writer}`, peer.writer, { encryptionKey: this.encryptionKey })
        this.cores.push(peerWriter)
        await this._joinSwarm(peerWriter, { server: false, client: true })
        await this.base.addInput(peerWriter)

        if(peer.meta !== this.remoteMetaCore.key.toString('hex')) {
          const peerMeta = new Hypercore(`${this.storage}/peers/${peer.meta}`, peer.meta)
          this.cores.push(peerMeta)
          await this._joinSwarm(peerMeta, { server: false, client: true })
          await this.metaBase.addInput(peerMeta)
        }
      }
    }
  }

  async collection(name) {
    await this.bee.sub(name)
    const collection = new Collection(name, this.bee)
    this.collections[name] = collection
    return collection
  }

  async addInput(key) {
    const core = new Hypercore(
      `${this.storage}/${key.toString('hex')}`,
      key,
      {
        encryptionKey: this.encryptionKey
      }
    )

    this.cores.push(core)

    await this.base.addInput(core)

    await this._joinSwarm(core, { server: false, client: true })
  }

  async removeInput(key) {
    // TODO
  }

  async close() {
    for await(const core of this.cores) {
      await core.close()
    }

    for await(const conn of this.connections) {
      await conn.destroy()
    }
  }

  async _joinSwarm(core, { server, client }) {
    const swarm = new Hyperswarm()
  
    await core.ready()
  
    swarm.on('connection', async (socket, info) => {
      socket.pipe(core.replicate(info.client)).pipe(socket)
    })
  
    const topic = core.discoveryKey;
    const discovery = swarm.join(topic, { server, client })
  
    this.connections.push(discovery)
  
    if (server) return await discovery.flushed()
  
    return await swarm.flush()
  }
}

// Collections/subs are broken in autobase
class Collection {
  constructor(sub, db) {
    this.sub = sub
    this.db = db
  }

  async put(key, value) {
    let val = { ...value, __sub: this.sub }
    await this.db.put(key, val)
  }

  async get(key) {
    return this.db.get(key, { sub: this.sub })
  }
}

// class Collection {
//   constructor(sub, db) {
//     this.sub = `__${sub}__`
//     this.db = db
//   }

//   async put(key, value) {
//     await this.db.put(this.sub + key, value)
//   }

//   async get(key) {
//     const result = await this.db.get(this.sub + key)

//     result.key = result.key.replace(this.sub, '')

//     return result
//   }
// }

module.exports = Database
